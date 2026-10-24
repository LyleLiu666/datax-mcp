// Package postgres publishes a verified stage and its commit receipt atomically.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Publisher struct {
	db      *pgx.Conn
	chunk   *state.Chunk
	target  string
	columns string
}

func Open(ctx context.Context, dsn string, c *state.Chunk) (*Publisher, error) {
	cfg, e := pgx.ParseConfig(dsn)
	if e != nil {
		return nil, errors.New("target_connection_invalid")
	}
	cfg.ConnectTimeout = 10 * time.Second
	db, e := pgx.ConnectConfig(ctx, cfg)
	if e != nil {
		return nil, errors.New("target_unavailable")
	}
	p := &Publisher{db: db, chunk: c}
	fail := func(code string) (*Publisher, error) { db.Close(ctx); return nil, errors.New(code) }
	schema, table, e := profile.TableParts(c.Profile.TargetTable)
	if e != nil {
		return fail("target_identifier_invalid")
	}
	p.target = pgx.Identifier{schema, table}.Sanitize()
	cols := []string{}
	for _, col := range c.Profile.Columns {
		cols = append(cols, pgx.Identifier{col}.Sanitize())
	}
	p.columns = strings.Join(cols, ",")
	if _, e = db.Exec(ctx, "SET statement_timeout='30s'; SET lock_timeout='5s'"); e != nil {
		return fail("target_session_failed")
	}
	var ok bool
	if e = db.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1,0))", "datax-mcp:"+c.Profile.TargetTable).Scan(&ok); e != nil || !ok {
		return fail("target_busy")
	}
	// Serialize metadata setup even when separate workers target different tables.
	if _, e = db.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended('datax-mcp:init',0))"); e != nil {
		return fail("target_metadata_busy")
	}
	_, e = db.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS datax_mcp_meta; CREATE SCHEMA IF NOT EXISTS datax_mcp_stage;
CREATE TABLE IF NOT EXISTS datax_mcp_meta.commits(run_id text NOT NULL,partition_id integer NOT NULL,profile text NOT NULL,lower_id bigint NOT NULL,upper_id bigint NOT NULL,row_count bigint NOT NULL,committed_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(run_id,partition_id));`)
	db.Exec(ctx, "SELECT pg_advisory_unlock(hashtextextended('datax-mcp:init',0))")
	if e != nil {
		return fail("target_metadata_failed")
	}
	return p, nil
}
func (p *Publisher) Close(ctx context.Context) { p.db.Close(ctx) }
func (p *Publisher) Committed(ctx context.Context) (int64, bool, error) {
	var rows int64
	e := p.db.QueryRow(ctx, "SELECT row_count FROM datax_mcp_meta.commits WHERE run_id=$1 AND partition_id=$2", p.chunk.RunID, p.chunk.Index).Scan(&rows)
	if errors.Is(e, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if e != nil {
		return 0, false, errors.New("target_receipt_failed")
	}
	return rows, true, nil
}
func (p *Publisher) Prepare(ctx context.Context) (string, error) {
	name := fmt.Sprintf("p_%s_%d_%s", strings.ReplaceAll(p.chunk.RunID, "-", "")[:12], p.chunk.Index, strings.ReplaceAll(uuid.NewString(), "-", "")[:12])
	stage := pgx.Identifier{"datax_mcp_stage", name}.Sanitize()
	// Separate stage per attempt: an orphaned old DataX process can never write
	// into the next attempt's stage or directly into the final table.
	_, e := p.db.Exec(ctx, "CREATE TABLE "+stage+" (LIKE "+p.target+")")
	if e != nil {
		return "", errors.New("stage_create_failed")
	}
	return stage, nil
}
func (p *Publisher) Cleanup(ctx context.Context, stage string) {
	p.db.Exec(ctx, "DROP TABLE IF EXISTS "+stage)
}
func (p *Publisher) Commit(ctx context.Context, stage string) (int64, error) {
	tx, e := p.db.Begin(ctx)
	if e != nil {
		return 0, errors.New("publish_begin_failed")
	}
	defer tx.Rollback(ctx)
	key := pgx.Identifier{p.chunk.Profile.Key}.Sanitize()
	var count, unique, bad int64
	e = tx.QueryRow(ctx, "SELECT count(*),count(DISTINCT "+key+"),count(*) FILTER(WHERE "+key+" IS NULL OR "+key+"<=$1 OR "+key+">$2) FROM "+stage, p.chunk.Lower, p.chunk.Upper).Scan(&count, &unique, &bad)
	if e != nil {
		return 0, errors.New("stage_key_check_failed")
	}
	if unique != count || bad > 0 {
		return 0, errors.New("stage_key_or_range_invalid")
	}
	assignments := []string{}
	for _, col := range p.chunk.Profile.Columns {
		if col != p.chunk.Profile.Key {
			q := pgx.Identifier{col}.Sanitize()
			assignments = append(assignments, q+"=EXCLUDED."+q)
		}
	}
	conflict := "DO NOTHING"
	if len(assignments) > 0 {
		conflict = "DO UPDATE SET " + strings.Join(assignments, ",")
	}
	_, e = tx.Exec(ctx, "INSERT INTO "+p.target+" ("+p.columns+") SELECT "+p.columns+" FROM "+stage+" ON CONFLICT ("+key+") "+conflict)
	if e != nil {
		return 0, errors.New("publish_merge_failed")
	}
	// Database-side equality compares all projected columns, including bytea and
	// NULL, without sending row contents through the controller or MCP.
	var mismatch int64
	e = tx.QueryRow(ctx, "SELECT count(*) FROM (SELECT "+p.columns+" FROM "+stage+" EXCEPT SELECT "+p.columns+" FROM "+p.target+" WHERE "+key+" IN(SELECT "+key+" FROM "+stage+")) diff").Scan(&mismatch)
	if e != nil || mismatch != 0 {
		return 0, errors.New("publish_verification_failed")
	}
	_, e = tx.Exec(ctx, "INSERT INTO datax_mcp_meta.commits(run_id,partition_id,profile,lower_id,upper_id,row_count) VALUES($1,$2,$3,$4,$5,$6)", p.chunk.RunID, p.chunk.Index, p.chunk.Profile.Name, p.chunk.Lower, p.chunk.Upper, count)
	if e != nil {
		return 0, errors.New("publish_receipt_failed")
	}
	if _, e = tx.Exec(ctx, "DROP TABLE "+stage); e != nil {
		return 0, errors.New("stage_cleanup_failed")
	}
	if e = tx.Commit(ctx); e != nil {
		return 0, errors.New("publish_commit_unknown_resume_to_recheck")
	}
	return count, nil
}
