// Package worker runs independently of an MCP connection, one partition at a time.
package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/LyleLiu666/datax-mcp/internal/datax"
	"github.com/LyleLiu666/datax-mcp/internal/postgres"
	"github.com/LyleLiu666/datax-mcp/internal/state"
)

type Worker struct {
	Store  *state.Store
	Runner datax.Runner
}

func (w Worker) Once(ctx context.Context) (bool, error) {
	c, e := w.Store.Next()
	if e != nil {
		return false, e
	}
	if c == nil {
		return false, nil
	}
	if e = w.process(ctx, c); e != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		} // running checkpoint is recovered by next worker
		// All process/driver diagnostics are private; persisted error codes come only
		// from controlled errors in the execution adapter, never from raw DataX logs.
		if fail := w.Store.Fail(c, e.Error()); fail != nil {
			return true, errors.New("state_write_failed")
		}
		return true, e
	}
	return true, nil
}
func (w Worker) process(ctx context.Context, c *state.Chunk) error {
	dsn, ok := os.LookupEnv(c.Profile.TargetDSNEnv)
	if !ok {
		return errors.New("target_environment_missing")
	}
	pub, e := postgres.Open(ctx, dsn, c)
	if e != nil {
		return e
	}
	defer pub.Close(context.Background())
	rows, done, e := pub.Committed(ctx)
	if e != nil {
		return e
	}
	if done {
		if e = w.Store.Complete(c, rows); e != nil {
			return errors.New("local_checkpoint_failed")
		}
		return nil
	}
	stage, e := pub.Prepare(ctx)
	if e != nil {
		return e
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pub.Cleanup(cleanup, stage)
	}()
	dir := filepath.Join(w.Store.Dir, "attempts", fmt.Sprintf("%s-%d-%d", c.RunID, c.Index, c.Attempt))
	if e = w.Runner.Load(ctx, c.Profile, c.Bounds, stage, dsn, dir); e != nil {
		return e
	}
	rows, e = pub.Commit(ctx, stage)
	if e != nil {
		return e
	}
	if e = w.Store.Complete(c, rows); e != nil {
		return errors.New("local_checkpoint_failed")
	}
	return nil
}

// Run holds an OS lock for its lifetime. A killed worker releases it; the new
// worker resumes unfinished partitions and checks destination receipts first.
func (w Worker) Run(ctx context.Context, once bool) error {
	lock, e := os.OpenFile(filepath.Join(w.Store.Dir, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return errors.New("worker_already_running")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if e = w.Runner.Check(); e != nil {
		return e
	}
	if e = w.Store.Recover(); e != nil {
		return e
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		worked, e := w.Once(ctx)
		if once {
			return e
		}
		if ctx.Err() != nil {
			return nil
		}
		// Failed runs are parked, not retried in a loop. Other queued profiles can run.
		if !worked || e != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
		}
	}
}
func Alive(dir string) bool {
	f, e := os.OpenFile(filepath.Join(dir, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return false
	}
	defer f.Close()
	e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if e != nil {
		return true
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}
