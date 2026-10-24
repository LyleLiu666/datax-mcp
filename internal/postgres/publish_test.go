package postgres

import (
	"context"
	"encoding/json"
	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"os"
	"testing"
)

func fixture(t *testing.T) (context.Context, string, *pgx.Conn, *state.Chunk) {
	t.Helper()
	dsn := os.Getenv("DATAX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DATAX_TEST_PG_DSN for isolated PostgreSQL integration")
	}
	ctx := context.Background()
	db, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	name := "test_" + uuid.NewString()[:8]
	_, e = db.Exec(ctx, "CREATE TABLE public."+name+"(id bigint PRIMARY KEY, body bytea)")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Exec(ctx, "DROP TABLE public."+name); db.Close(ctx) })
	p := profile.Profile{Name: name, TargetTable: "public." + name, TargetDSNEnv: "DATAX_TEST_PG_DSN", Key: "id", Columns: []string{"id", "body"}, ChunkSize: 10, Reader: json.RawMessage(`{"name":"streamreader","parameter":{}}`)}
	c := &state.Chunk{RunID: uuid.NewString(), Index: 0, Bounds: profile.Bounds{Lower: 0, Upper: 10}, Profile: p}
	return ctx, dsn, db, c
}
func TestPublishAndCrashAfterCommitReplay(t *testing.T) {
	ctx, dsn, db, c := fixture(t)
	p, e := Open(ctx, dsn, c)
	if e != nil {
		t.Fatal(e)
	}
	stage, e := p.Prepare(ctx)
	if e != nil {
		t.Fatal(e)
	}
	body := []byte("原文\x00😀")
	_, e = db.Exec(ctx, "INSERT INTO "+stage+"(id,body) VALUES(1,$1)", body)
	if e != nil {
		t.Fatal(e)
	}
	n, e := p.Commit(ctx, stage)
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	p.Close(ctx)
	// Simulate destination COMMIT succeeding before the local checkpoint is saved.
	p, e = Open(ctx, dsn, c)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close(ctx)
	rows, done, e := p.Committed(ctx)
	if e != nil || !done || rows != 1 {
		t.Fatal(rows, done, e)
	}
	var got []byte
	db.QueryRow(ctx, "SELECT body FROM "+c.Profile.TargetTable+" WHERE id=1").Scan(&got)
	if string(got) != string(body) {
		t.Fatal("body changed")
	}
}
func TestBadPartitionCannotChangeTarget(t *testing.T) {
	ctx, dsn, db, c := fixture(t)
	p, e := Open(ctx, dsn, c)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close(ctx)
	stage, e := p.Prepare(ctx)
	if e != nil {
		t.Fatal(e)
	}
	db.Exec(ctx, "INSERT INTO "+stage+" VALUES (11,'outside'::bytea)")
	if _, e = p.Commit(ctx, stage); e == nil {
		t.Fatal("out-of-range row published")
	}
	var n int
	db.QueryRow(ctx, "SELECT count(*) FROM "+c.Profile.TargetTable).Scan(&n)
	if n != 0 {
		t.Fatal("target changed")
	}
	_, done, _ := p.Committed(ctx)
	if done {
		t.Fatal("checkpoint advanced")
	}
	p.Cleanup(ctx, stage)
}
