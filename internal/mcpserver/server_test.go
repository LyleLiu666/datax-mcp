package mcpserver

import (
	"context"
	"encoding/json"
	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"strings"
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

func TestMCPRegisterPreservesInt64Profile(t *testing.T) {
	ctx := context.Background()
	s, e := state.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	a, b := mcp.NewInMemoryTransports()
	ss, e := New(s, "").Connect(ctx, a, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer ss.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "precision-test", Version: "1"}, nil)
	cs, e := client.Connect(ctx, b, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer cs.Close()
	p := map[string]any{"name": "large", "target_table": "public.demo", "target_dsn_env": "DEMO_DSN", "key": "id", "columns": []string{"id"}, "chunk_size": int64(9223372036854775807), "reader": map[string]any{"name": "streamreader", "parameter": map[string]any{}}}
	result, e := cs.CallTool(ctx, &mcp.CallToolParams{Name: "sync_register", Arguments: map[string]any{"profile": p}})
	if e != nil || result.IsError {
		for _, c := range result.Content {
			t.Logf("%+v", c)
		}
		t.Fatal(result, e)
	}
	got, e := s.Profile("large")
	if e != nil || got.ChunkSize != 9223372036854775807 {
		t.Fatal(got, e)
	}
	result, e = cs.CallTool(ctx, &mcp.CallToolParams{Name: "sync_profiles", Arguments: map[string]any{}})
	if e != nil || result.IsError {
		for _, c := range result.Content {
			t.Logf("%+v", c)
		}
		t.Fatal(result, e)
	}
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok && !strings.Contains(text.Text, "9223372036854775807") {
			t.Fatal("profile precision lost")
		}
	}
}

func TestMCPStartPreservesLargeBounds(t *testing.T) {
	ctx := context.Background()
	s, e := state.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p := profile.Profile{Name: "large", TargetTable: "public.demo", TargetDSNEnv: "DEMO_DSN", Key: "id", Columns: []string{"id"}, ChunkSize: 9223372036854775807, Reader: json.RawMessage(`{"name":"streamreader","parameter":{}}`)}
	if e = s.PutProfile(p); e != nil {
		t.Fatal(e)
	}
	a, b := mcp.NewInMemoryTransports()
	ss, e := New(s, "").Connect(ctx, a, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer ss.Close()
	cs, e := mcp.NewClient(&mcp.Implementation{Name: "bounds-test", Version: "1"}, nil).Connect(ctx, b, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer cs.Close()
	result, e := cs.CallTool(ctx, &mcp.CallToolParams{Name: "sync_start", Arguments: map[string]any{"profile": "large", "mode": "full", "lower": int64(9007199254740993), "upper": int64(9223372036854775807)}})
	if e != nil || result.IsError {
		t.Fatal(result, e)
	}
	runs, e := s.Runs()
	if e != nil || len(runs) != 1 || runs[0].Lower != 9007199254740993 || runs[0].Upper != 9223372036854775807 {
		t.Fatal(runs, e)
	}
	for _, c := range result.Content {
		if v, ok := c.(*mcp.TextContent); ok && (!strings.Contains(v.Text, "9007199254740993") || !strings.Contains(v.Text, "9223372036854775807")) {
			t.Fatal("result bounds changed")
		}
	}
}
