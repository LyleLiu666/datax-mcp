package state

import (
	"encoding/json"
	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"os"
	"path/filepath"
	"testing"
)

func fixture(t *testing.T) *Store {
	t.Helper()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	p := profile.Profile{Name: "demo", TargetTable: "public.demo", TargetDSNEnv: "TEST_DSN", Key: "id", Columns: []string{"id"}, ChunkSize: 10, Reader: json.RawMessage(`{"name":"streamreader","parameter":{"column":[{"type":"long","value":"{{first}}"}],"sliceRecordCount":1}}`)}
	if e = s.PutProfile(p); e != nil {
		t.Fatal(e)
	}
	return s
}
func TestPauseResumeAndPersistentPartitionBoundary(t *testing.T) {
	s := fixture(t)
	r, e := s.Start("demo", "full", 0, 25)
	if e != nil {
		t.Fatal(e)
	}
	c, e := s.Next()
	if e != nil || c == nil {
		t.Fatal(e)
	}
	if e = s.Pause(r.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Complete(c, 3); e != nil {
		t.Fatal(e)
	}
	got, _ := s.GetRun(r.ID)
	if got.Status != "paused" || got.Completed != 1 || got.LastID != 10 {
		t.Fatalf("%+v", got)
	}
	if c, _ := s.Next(); c != nil {
		t.Fatal("paused run still scheduled")
	}
	if e = s.Resume(r.ID); e != nil {
		t.Fatal(e)
	}
	c, _ = s.Next()
	if c.Lower != 10 || c.Upper != 20 {
		t.Fatalf("%+v", c)
	}
	s.Fail(c, "synthetic_failure")
	got, _ = s.GetRun(r.ID)
	if got.Status != "failed" || got.Completed != 1 {
		t.Fatalf("%+v", got)
	}
	if e = s.Resume(r.ID); e != nil {
		t.Fatal(e)
	}
	c, _ = s.Next()
	if c.Lower != 10 {
		t.Fatal("failed partition was skipped")
	}
	s.Complete(c, 2)
	c, _ = s.Next()
	s.Complete(c, 1)
	got, _ = s.GetRun(r.ID)
	if got.Status != "completed" || got.Rows != 6 || got.LastID != 25 {
		t.Fatalf("%+v", got)
	}
	next, e := s.Start("demo", "incremental", 0, 30)
	if e != nil || next.Lower != 25 {
		t.Fatalf("%+v %v", next, e)
	}
}
func TestActiveRunBlocksProfileMutationAndDuplicateStart(t *testing.T) {
	s := fixture(t)
	r, _ := s.Start("demo", "full", 0, 10)
	if _, e := s.Start("demo", "full", 0, 10); e == nil {
		t.Fatal("duplicate run")
	}
	p, _ := s.Profile("demo")
	p.ChunkSize = 5
	if s.PutProfile(p) == nil {
		t.Fatal("inflight profile changed")
	}
	s.Pause(r.ID)
	if _, e := s.Start("demo", "incremental", 0, 20); e == nil {
		t.Fatal("unfinished run skipped")
	}
}
func TestRecoveryRetriesInflightButKeepsCompleted(t *testing.T) {
	s := fixture(t)
	r, _ := s.Start("demo", "full", 0, 20)
	c, _ := s.Next()
	s.Complete(c, 2)
	s.Next()
	if e := s.Recover(); e != nil {
		t.Fatal(e)
	}
	c, _ = s.Next()
	if c == nil || c.Lower != 10 {
		t.Fatal(c)
	}
	got, _ := s.GetRun(r.ID)
	if got.Completed != 1 {
		t.Fatal(got)
	}
}

func TestExistingStateDirectoryIsPrivate(t *testing.T) {
	dir := t.TempDir()
	if e := os.Chmod(dir, 0755); e != nil {
		t.Fatal(e)
	}
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	info, _ := os.Stat(dir)
	if info.Mode().Perm() != 0700 {
		t.Fatalf("state directory mode %o", info.Mode().Perm())
	}
	info, _ = os.Stat(filepath.Join(dir, "state.sqlite"))
	if info.Mode().Perm() != 0600 {
		t.Fatalf("state file mode %o", info.Mode().Perm())
	}
}
