// Package mcpserver exposes the control plane. DataX is never run in a tool call.
package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/LyleLiu666/datax-mcp/internal/datax"
	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/LyleLiu666/datax-mcp/internal/worker"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const Version = "0.1.0"

type Empty struct{}
type RegisterInput struct {
	Profile json.RawMessage `json:"profile" jsonschema:"Approved profile with native reader, target table, environment reference, key, columns, and chunk_size; never include literal credentials"`
}
type StartInput struct {
	Profile string `json:"profile"`
	Mode    string `json:"mode" jsonschema:"full or incremental; incremental starts at the last successfully completed run's upper bound"`
	Lower   int64  `json:"lower,omitempty" jsonschema:"Exclusive lower ID bound for full runs"`
	Upper   int64  `json:"upper" jsonschema:"Inclusive fixed upper ID bound observed in the source"`
}
type RunInput struct {
	RunID string `json:"run_id"`
}
type StatusInput struct {
	RunID string `json:"run_id,omitempty"`
}
type ProfilesOutput struct {
	Profiles []map[string]any `json:"profiles"`
}
type RunsOutput struct {
	Runs          []state.Run `json:"runs"`
	WorkerRunning bool        `json:"worker_running"`
}
type Registered struct {
	Name string `json:"name"`
}
type Capabilities struct {
	Version        string   `json:"version"`
	Readers        []string `json:"readers"`
	Writer         string   `json:"writer"`
	WorkerRunning  bool     `json:"worker_running"`
	RuntimeReady   bool     `json:"runtime_ready"`
	ResumeBoundary string   `json:"resume_boundary"`
	Incremental    string   `json:"incremental"`
	Limitations    []string `json:"limitations"`
}

func New(s *state.Store, home string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "datax-mcp", Version: Version}, nil)
	addExactTool(server, &mcp.Tool{Name: "sync_capabilities", Description: "Inspect runtime readiness, worker availability, supported transfers, and correctness boundaries."}, func(ctx context.Context, r *mcp.CallToolRequest, in Empty) (*mcp.CallToolResult, Capabilities, error) {
		return nil, Capabilities{Version: Version, Readers: []string{"mysqlreader", "streamreader (synthetic fixtures)"}, Writer: "postgresqlwriter with transactional staged publication", WorkerRunning: worker.Alive(s.Dir), RuntimeReady: (datax.Runner{Home: home}).Check() == nil, ResumeBoundary: "last verified and committed ID-range partition", Incremental: "append by integer ID high-water mark; explicit full reruns can refresh old rows", Limitations: []string{"not CDC", "no automatic source-delete propagation", "no cluster scheduler; one independent worker per state directory", "source field fidelity requires a verified reader projection; timestamp precision needs explicit transport mapping"}}, nil
	})
	addExactTool(server, &mcp.Tool{Name: "sync_register", Description: "Register an immutable, operator-approved transfer profile. Credentials are environment references resolved only by the worker.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"profile": map[string]any{"type": "object"}}, "required": []string{"profile"}, "additionalProperties": false}}, func(ctx context.Context, r *mcp.CallToolRequest, in RegisterInput) (*mcp.CallToolResult, Registered, error) {
		raw := in.Profile
		if !json.Valid(raw) || len(raw) > 1024*1024 {
			return nil, Registered{}, errors.New("invalid profile payload")
		}
		var p profile.Profile
		if e := json.Unmarshal(raw, &p); e != nil {
			return nil, Registered{}, errors.New("invalid profile schema")
		}
		if e := s.PutProfile(p); e != nil {
			return nil, Registered{}, e
		}
		return nil, Registered{Name: p.Name}, nil
	})
	addExactTool(server, &mcp.Tool{Name: "sync_profiles", Description: "List registered transfer profiles; only environment references, never resolved credentials."}, func(ctx context.Context, r *mcp.CallToolRequest, in Empty) (*mcp.CallToolResult, ProfilesOutput, error) {
		p, e := s.Profiles()
		out := ProfilesOutput{Profiles: []map[string]any{}}
		if e != nil {
			return nil, out, e
		}
		for _, item := range p {
			raw, err := json.Marshal(item)
			if err != nil {
				return nil, out, err
			}
			var value map[string]any
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if err = decoder.Decode(&value); err != nil {
				return nil, out, err
			}
			out.Profiles = append(out.Profiles, value)
		}
		return nil, out, nil
	})
	addExactTool(server, &mcp.Tool{Name: "sync_start", Description: "Queue a bounded full or incremental run and return immediately. The separate worker continues after this MCP client disconnects."}, func(ctx context.Context, r *mcp.CallToolRequest, in StartInput) (*mcp.CallToolResult, state.Run, error) {
		out, e := s.Start(in.Profile, in.Mode, in.Lower, in.Upper)
		return nil, out, e
	})
	addExactTool(server, &mcp.Tool{Name: "sync_status", Description: "Read progress and verification counts for a run, or list the latest 50 runs. Does not expose raw rows or DataX logs."}, func(ctx context.Context, r *mcp.CallToolRequest, in StatusInput) (*mcp.CallToolResult, RunsOutput, error) {
		out := RunsOutput{WorkerRunning: worker.Alive(s.Dir)}
		var e error
		if in.RunID == "" {
			out.Runs, e = s.Runs()
		} else {
			var run state.Run
			run, e = s.GetRun(in.RunID)
			out.Runs = []state.Run{run}
		}
		return nil, out, e
	})
	addExactTool(server, &mcp.Tool{Name: "sync_pause", Description: "Pause at a partition boundary. An already-running partition is allowed to finish and commit; completed data is preserved."}, func(ctx context.Context, r *mcp.CallToolRequest, in RunInput) (*mcp.CallToolResult, state.Run, error) {
		if e := s.Pause(in.RunID); e != nil {
			return nil, state.Run{}, e
		}
		out, e := s.GetRun(in.RunID)
		return nil, out, e
	})
	addExactTool(server, &mcp.Tool{Name: "sync_resume", Description: "Resume a paused or failed run. Completed partitions are not re-read; a destination receipt resolves a crash after commit."}, func(ctx context.Context, r *mcp.CallToolRequest, in RunInput) (*mcp.CallToolResult, state.Run, error) {
		if e := s.Resume(in.RunID); e != nil {
			return nil, state.Run{}, e
		}
		out, e := s.GetRun(in.RunID)
		return nil, out, e
	})
	return server
}
