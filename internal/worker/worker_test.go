package worker

import (
	"context"
	"encoding/json"
	"github.com/LyleLiu666/datax-mcp/internal/datax"
	"github.com/LyleLiu666/datax-mcp/internal/postgres"
	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"os"
	"path/filepath"
	"testing"
)

func TestRealDataXWorkerPreservesBinaryContent(t *testing.T) {
	home := os.Getenv("DATAX_HOME")
	dsn := os.Getenv("DATAX_TEST_PG_DSN")
	if home == "" || dsn == "" {
		t.Skip("requires actual DataX and isolated PostgreSQL")
	}
	ctx := context.Background()
	db, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(ctx)
	table := "worker_" + uuid.NewString()[:8]
	db.Exec(ctx, "CREATE TABLE public."+table+"(id bigint PRIMARY KEY, body bytea)")
	defer db.Exec(ctx, "DROP TABLE public."+table)
	s, e := state.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	reader, _ := json.Marshal(map[string]any{"name": "streamreader", "parameter": map[string]any{"column": []any{map[string]any{"type": "long", "value": "{{first}}"}, map[string]any{"type": "bytes", "value": "中文\x00😀"}}, "sliceRecordCount": 1}})
	p := profile.Profile{Name: "test", Reader: reader, TargetTable: "public." + table, TargetDSNEnv: "DATAX_TEST_PG_DSN", Key: "id", Columns: []string{"id", "body"}, ChunkSize: 10, TimeoutSeconds: 90}
	if e = s.PutProfile(p); e != nil {
		t.Fatal(e)
	}
	r, e := s.Start(p.Name, "full", 0, 20)
	if e != nil {
		t.Fatal(e)
	}
	w := Worker{Store: s, Runner: datax.Runner{Home: home}}
	for i := 0; i < 2; i++ {
		worked, e := w.Once(ctx)
		if e != nil || !worked {
			logs, _ := filepath.Glob(filepath.Join(s.Dir, "attempts", "*", "datax.log"))
			for _, file := range logs {
				b, _ := os.ReadFile(file)
				t.Log(string(b))
			}
			t.Fatalf("%v %v; diagnostics under %s", worked, e, s.Dir)
		}
	}
	got, _ := s.GetRun(r.ID)
	if got.Completed != 2 || got.Rows != 2 || got.Status != "completed" {
		t.Fatalf("%+v", got)
	}
	var body []byte
	db.QueryRow(ctx, "SELECT body FROM public."+table+" WHERE id=1").Scan(&body)
	if string(body) != "中文\x00😀" {
		t.Fatalf("body did not round trip")
	}
	matches, _ := filepath.Glob(filepath.Join(s.Dir, "attempts", "*", "job.json"))
	if len(matches) != 0 {
		t.Fatal("resolved credential config retained")
	}
}

func TestRealMySQLProjectionAndFailureResume(t *testing.T) {
	home, dsn, mysqlURL := os.Getenv("DATAX_HOME"), os.Getenv("DATAX_TEST_PG_DSN"), os.Getenv("DATAX_TEST_MYSQL_JDBC")
	if home == "" || dsn == "" || mysqlURL == "" {
		t.Skip("requires DataX, PostgreSQL and scripts/mysql-fixture.sql")
	}
	ctx := context.Background()
	db, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(ctx)
	table := "mysql_" + uuid.NewString()[:8]
	if _, e = db.Exec(ctx, "CREATE TABLE public."+table+"(id bigint PRIMARY KEY,body bytea,label text,created_at text,amount text)"); e != nil {
		t.Fatal(e)
	}
	defer db.Exec(ctx, "DROP TABLE public."+table)
	s, e := state.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	// CHAR transports preserve decimal and microsecond values; native JDBC date
	// conversion is deliberately not used as evidence of source fidelity.
	reader := json.RawMessage(`{"name":"mysqlreader","parameter":{"username":"sync_reader","password":"${DATAX_FIXTURE_PASSWORD}","session":["SET SESSION MAX_EXECUTION_TIME=5000"],"connection":[{"jdbcUrl":["${DATAX_TEST_MYSQL_JDBC}"],"querySql":["SELECT id,body,label,CAST(created_at AS CHAR),CAST(amount AS CHAR) FROM source_items WHERE id > {{lower}} AND id <= {{upper}} ORDER BY id"]}]}}`)
	p := profile.Profile{Name: "mysql-test", Reader: reader, TargetTable: "public." + table, TargetDSNEnv: "DATAX_TEST_PG_DSN", Key: "id", Columns: []string{"id", "body", "label", "created_at", "amount"}, ChunkSize: 5000000000000000, TimeoutSeconds: 90}
	if e = s.PutProfile(p); e != nil {
		t.Fatal(e)
	}
	r, e := s.Start(p.Name, "full", 0, 9007199254740993)
	if e != nil {
		t.Fatal(e)
	}
	w := Worker{Store: s, Runner: datax.Runner{Home: home}}
	t.Setenv("DATAX_FIXTURE_PASSWORD", "fixture-reader")
	// A missing worker-side secret must fail without advancing the partition.
	os.Unsetenv("DATAX_FIXTURE_PASSWORD")
	if _, e = w.Once(ctx); e == nil {
		t.Fatal("missing secret succeeded")
	}
	failed, _ := s.GetRun(r.ID)
	if failed.Status != "failed" || failed.Completed != 0 {
		t.Fatal(failed)
	}
	os.Setenv("DATAX_FIXTURE_PASSWORD", "fixture-reader")
	if e = s.Resume(r.ID); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if _, e = w.Once(ctx); e != nil {
			logs, _ := filepath.Glob(filepath.Join(s.Dir, "attempts", "*", "datax.log"))
			for _, f := range logs {
				b, _ := os.ReadFile(f)
				t.Log(string(b))
			}
			t.Fatal(e)
		}
		if i == 0 {
			if e = s.Pause(r.ID); e != nil {
				t.Fatal(e)
			}
			if worked, e := w.Once(ctx); worked || e != nil {
				t.Fatal("paused run advanced")
			}
			if e = s.Resume(r.ID); e != nil {
				t.Fatal(e)
			}
		}
	}
	got, _ := s.GetRun(r.ID)
	if got.Status != "completed" || got.Rows != 3 || got.Completed != 2 {
		t.Fatal(got)
	}
	var body []byte
	var label, created, amount string
	if e = db.QueryRow(ctx, "SELECT body,label,created_at,amount FROM public."+table+" WHERE id=1").Scan(&body, &label, &created, &amount); e != nil {
		t.Fatal(e)
	}
	if string(body) != "中文\x00😀" || label != "" || created != "2026-01-02 03:04:05.123456" || amount != "123456789012345678.123456789012" {
		t.Fatalf("projection mismatch: %q %q %q %q", body, label, created, amount)
	}
	var allNull bool
	if e = db.QueryRow(ctx, "SELECT body IS NULL AND label IS NULL AND created_at IS NULL AND amount IS NULL FROM public."+table+" WHERE id=3").Scan(&allNull); e != nil || !allNull {
		t.Fatal("NULL collapsed", e)
	}
	if e = db.QueryRow(ctx, "SELECT body,created_at,amount FROM public."+table+" WHERE id=9007199254740993").Scan(&body, &created, &amount); e != nil {
		t.Fatal(e)
	}
	if body == nil || len(body) != 0 || created != "2026-01-02 03:04:05.000001" || amount != "0.000000000001" {
		t.Fatal("empty bytes, large ID or precision changed")
	}
}

func TestRecoveryAfterDestinationCommitSkipsDataX(t *testing.T) {
	dsn := os.Getenv("DATAX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
	}
	ctx := context.Background()
	db, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(ctx)
	table := "receipt_" + uuid.NewString()[:8]
	if _, e = db.Exec(ctx, "CREATE TABLE public."+table+"(id bigint PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	defer db.Exec(ctx, "DROP TABLE public."+table)
	dir := t.TempDir()
	s, e := state.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	p := profile.Profile{Name: "receipt-test", Reader: json.RawMessage(`{"name":"streamreader","parameter":{}}`), TargetTable: "public." + table, TargetDSNEnv: "DATAX_TEST_PG_DSN", Key: "id", Columns: []string{"id"}, ChunkSize: 10}
	if e = s.PutProfile(p); e != nil {
		t.Fatal(e)
	}
	r, e := s.Start(p.Name, "full", 0, 10)
	if e != nil {
		t.Fatal(e)
	}
	c, e := s.Next()
	if e != nil {
		t.Fatal(e)
	}
	pub, e := postgres.Open(ctx, dsn, c)
	if e != nil {
		t.Fatal(e)
	}
	stage, e := pub.Prepare(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(ctx, "INSERT INTO "+stage+" VALUES(1)"); e != nil {
		t.Fatal(e)
	}
	if _, e = pub.Commit(ctx, stage); e != nil {
		t.Fatal(e)
	}
	pub.Close(ctx)
	// Process loss here: no Store.Complete, no reusable in-memory state.
	s.Close()
	s, e = state.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Recover(); e != nil {
		t.Fatal(e)
	}
	w := Worker{Store: s, Runner: datax.Runner{Home: "/deliberately/missing/runtime"}}
	if worked, e := w.Once(ctx); e != nil || !worked {
		t.Fatal(worked, e)
	}
	got, e := s.GetRun(r.ID)
	if e != nil || got.Status != "completed" || got.Rows != 1 {
		t.Fatal(got, e)
	}
	var count int
	if e = db.QueryRow(ctx, "SELECT count(*) FROM public."+table).Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
}
