package mcpserver

import (
	"context"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"testing"
)

func TestMCPProtocolToolsAndRunLifecycle(t *testing.T) {
	ctx := context.Background()
	s, e := state.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	server := New(s, "")
	a, b := mcp.NewInMemoryTransports()
	ss, e := server.Connect(ctx, a, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer ss.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	cs, e := client.Connect(ctx, b, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer cs.Close()
	list, e := cs.ListTools(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(list.Tools) != 7 {
		t.Fatalf("got %d tools", len(list.Tools))
	}
	p := map[string]any{"name": "demo", "target_table": "public.demo", "target_dsn_env": "DEMO_DSN", "key": "id", "columns": []string{"id"}, "chunk_size": 10, "reader": map[string]any{"name": "streamreader", "parameter": map[string]any{}}}
	call := func(name string, args map[string]any) *mcp.CallToolResult {
		t.Helper()
		r, e := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if e != nil || r.IsError {
			t.Fatalf("%s: %+v %v", name, r, e)
		}
		return r
	}
	call("sync_register", map[string]any{"profile": p})
	call("sync_start", map[string]any{"profile": "demo", "mode": "full", "lower": 0, "upper": 25})
	runs, _ := s.Runs()
	if len(runs) != 1 || runs[0].Total != 3 {
		t.Fatal(runs)
	}
	call("sync_pause", map[string]any{"run_id": runs[0].ID})
	call("sync_resume", map[string]any{"run_id": runs[0].ID})
	call("sync_status", map[string]any{"run_id": runs[0].ID})
	call("sync_profiles", map[string]any{})
	call("sync_capabilities", map[string]any{})
}
