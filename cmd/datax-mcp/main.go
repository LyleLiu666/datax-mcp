// datax-mcp offers an MCP stdio server and a separately supervised worker.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/LyleLiu666/datax-mcp/internal/datax"
	"github.com/LyleLiu666/datax-mcp/internal/mcpserver"
	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/LyleLiu666/datax-mcp/internal/state"
	"github.com/LyleLiu666/datax-mcp/internal/worker"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: datax-mcp <serve|worker|register|start|status|pause|resume|doctor|version> [flags]")
	}
	command := args[0]
	if command == "version" {
		return json.NewEncoder(out).Encode(map[string]string{"version": mcpserver.Version})
	}
	switch command {
	case "serve", "worker", "register", "start", "status", "pause", "resume", "doctor":
	default:
		return errors.New("unknown command")
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	home, _ := os.UserHomeDir()
	defaultDir := os.Getenv("DATAX_MCP_STATE")
	if defaultDir == "" {
		defaultDir = filepath.Join(home, ".local", "state", "datax-mcp")
	}
	dir := flags.String("state", defaultDir, "private durable state directory")
	runtime := flags.String("datax-home", os.Getenv("DATAX_HOME"), "DataX runtime directory")
	java := flags.String("java", os.Getenv("DATAX_JAVA"), "Java executable (default java)")
	once := flags.Bool("once", false, "process at most one partition")
	file := flags.String("file", "", "approved profile JSON")
	name := flags.String("profile", "", "registered profile name")
	mode := flags.String("mode", "full", "full or incremental")
	lower := flags.Int64("lower", 0, "exclusive lower ID")
	upper := flags.Int64("upper", 0, "inclusive upper ID")
	id := flags.String("run", "", "logical run ID")
	if err := flags.Parse(args[1:]); err != nil {
		return errors.New("invalid flags; see README for command examples")
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *runtime != "" {
		p, e := filepath.Abs(*runtime)
		if e != nil {
			return e
		}
		*runtime = p
	}
	s, err := state.Open(*dir)
	if err != nil {
		return errors.New("cannot open private state directory")
	}
	defer s.Close()
	runner := datax.Runner{Home: *runtime, Java: *java}
	emit := func(v any, e error) error {
		if e != nil {
			return e
		}
		return json.NewEncoder(out).Encode(v)
	}
	switch command {
	case "serve":
		return mcpserver.New(s, *runtime).Run(ctx, &mcp.StdioTransport{})
	case "worker":
		return (worker.Worker{Store: s, Runner: runner}).Run(ctx, *once)
	case "doctor":
		return emit(map[string]any{"version": mcpserver.Version, "runtime_ready": runner.Check() == nil, "worker_running": worker.Alive(s.Dir), "state": s.Dir}, nil)
	case "register":
		f, e := os.Open(*file)
		if e != nil {
			return errors.New("cannot open profile file")
		}
		defer f.Close()
		var p profile.Profile
		decoder := json.NewDecoder(io.LimitReader(f, 1048577))
		decoder.DisallowUnknownFields()
		if e = decoder.Decode(&p); e != nil {
			return errors.New("invalid profile JSON")
		}
		if e = s.PutProfile(p); e != nil {
			return e
		}
		return emit(map[string]string{"name": p.Name}, nil)
	case "start":
		v, e := s.Start(*name, *mode, *lower, *upper)
		return emit(v, e)
	case "status":
		if *id != "" {
			v, e := s.GetRun(*id)
			return emit(v, e)
		}
		v, e := s.Runs()
		return emit(v, e)
	case "pause":
		if e := s.Pause(*id); e != nil {
			return e
		}
		v, e := s.GetRun(*id)
		return emit(v, e)
	case "resume":
		if e := s.Resume(*id); e != nil {
			return e
		}
		v, e := s.GetRun(*id)
		return emit(v, e)
	}
	return nil
}
