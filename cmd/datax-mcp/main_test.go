package main

import (
	"bytes"
	"context"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCLIRejectsUnknownCommandAndEmitsCapabilities(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"nonsense"}, &out); err == nil {
		t.Fatal("unknown command accepted")
	}
	if err := run(context.Background(), []string{"version"}, &out); err != nil || !bytes.Contains(out.Bytes(), []byte("0.1.0")) {
		t.Fatal(out.String(), err)
	}
}

func TestStdioDisconnectDoesNotLoseQueuedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "datax-mcp")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	if b, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build: %s %v", b, e)
	}
	dir := t.TempDir()
	command := exec.CommandContext(ctx, binary, "serve", "--state", dir)
	command.Stderr = os.Stderr
	client := mcp.NewClient(&mcp.Implementation{Name: "stdio-test", Version: "1"}, nil)
	session, e := client.Connect(ctx, &mcp.CommandTransport{Command: command}, nil)
	if e != nil {
		t.Fatal(e)
	}
	listed, e := session.ListTools(ctx, nil)
	if e != nil || len(listed.Tools) != 7 {
		t.Fatal(listed, e)
	}
	table := "stdio_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:10]
	p := map[string]any{"name": "stdio-test", "target_table": "public." + table, "target_dsn_env": "DATAX_TEST_PG_DSN", "key": "id", "columns": []string{"id"}, "chunk_size": 10, "reader": map[string]any{"name": "streamreader", "parameter": map[string]any{"column": []any{map[string]any{"type": "long", "value": "{{first}}"}}, "sliceRecordCount": 1}}}
	for _, call := range []*mcp.CallToolParams{{Name: "sync_register", Arguments: map[string]any{"profile": p}}, {Name: "sync_start", Arguments: map[string]any{"profile": "stdio-test", "mode": "full", "upper": 10}}} {
		result, err := session.CallTool(ctx, call)
		if err != nil || result.IsError {
			t.Fatal(result, err)
		}
	}
	if e = session.Close(); e != nil {
		t.Fatal(e)
	}
	s, e := state.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	runs, e := s.Runs()
	if e != nil || len(runs) != 1 || runs[0].Status != "queued" {
		t.Fatal(runs, e)
	}
	if os.Getenv("DATAX_HOME") == "" || os.Getenv("DATAX_TEST_PG_DSN") == "" {
		return
	}
	db, e := pgx.Connect(ctx, os.Getenv("DATAX_TEST_PG_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(ctx)
	if _, e = db.Exec(ctx, "CREATE TABLE public."+table+"(id bigint PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	defer db.Exec(ctx, "DROP TABLE public."+table)
	// The MCP process has exited. A different process consumes its durable run.
	worker := exec.CommandContext(ctx, binary, "worker", "--once", "--state", dir)
	if b, e := worker.CombinedOutput(); e != nil {
		t.Fatalf("worker: %s %v", b, e)
	}
	got, e := s.GetRun(runs[0].ID)
	if e != nil || got.Status != "completed" || got.Rows != 1 {
		t.Fatal(got, e)
	}
}
