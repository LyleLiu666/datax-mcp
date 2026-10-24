// Package state owns durable profiles, logical runs, and committed partitions.
package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type Store struct {
	DB  *sql.DB
	Dir string
}
type Run struct {
	ID        string `json:"id"`
	Profile   string `json:"profile"`
	Mode      string `json:"mode"`
	Status    string `json:"status"`
	Lower     int64  `json:"lower"`
	Upper     int64  `json:"upper"`
	LastID    int64  `json:"last_id"`
	Total     int    `json:"partitions"`
	Completed int    `json:"completed_partitions"`
	Rows      int64  `json:"verified_rows"`
	Error     string `json:"error_code,omitempty"`
	Created   string `json:"created_at"`
	Updated   string `json:"updated_at"`
}
type Chunk struct {
	RunID string
	Index int
	profile.Bounds
	Attempt int
	Profile profile.Profile
}

func Open(dir string) (*Store, error) {
	abs, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(abs, 0700); e != nil {
		return nil, e
	}
	if e = os.Chmod(abs, 0700); e != nil {
		return nil, e
	}
	path := filepath.Join(abs, "state.sqlite")
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = f.Chmod(0600); e != nil {
		f.Close()
		return nil, e
	}
	f.Close()
	db, e := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`CREATE TABLE IF NOT EXISTS profiles(name TEXT PRIMARY KEY, config TEXT NOT NULL, watermark INTEGER);
CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY, profile TEXT NOT NULL REFERENCES profiles(name), config TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL, lower_id INTEGER NOT NULL, upper_id INTEGER NOT NULL, error_code TEXT NOT NULL DEFAULT '', created TEXT NOT NULL, updated TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_unfinished_run ON runs(profile) WHERE status<>'completed';
CREATE TABLE IF NOT EXISTS chunks(run_id TEXT NOT NULL REFERENCES runs(id), idx INTEGER NOT NULL, lower_id INTEGER NOT NULL, upper_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', attempt INTEGER NOT NULL DEFAULT 0, row_count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(run_id,idx));`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Store{db, abs}, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func stamp() string           { return time.Now().UTC().Format(time.RFC3339Nano) }
func (s *Store) PutProfile(p profile.Profile) error {
	if e := p.Validate(); e != nil {
		return e
	}
	b, _ := json.Marshal(p)
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var n int
	if e = tx.QueryRow("SELECT count(*) FROM runs WHERE profile=? AND status<>'completed'", p.Name).Scan(&n); e != nil {
		return e
	}
	if n > 0 {
		return errors.New("profile has an unfinished run")
	}
	// Once used, a profile is immutable: a changed reader/target must get a new
	// name so an old watermark can never silently apply to a different dataset.
	var old string
	e = tx.QueryRow("SELECT config FROM profiles WHERE name=?", p.Name).Scan(&old)
	if e == nil && old != string(b) {
		return errors.New("profile names are immutable; register a new name")
	}
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = tx.Exec("INSERT INTO profiles(name,config) VALUES(?,?) ON CONFLICT(name) DO NOTHING", p.Name, string(b))
	if e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) Profile(name string) (profile.Profile, error) {
	var raw string
	p := profile.Profile{}
	e := s.DB.QueryRow("SELECT config FROM profiles WHERE name=?", name).Scan(&raw)
	if e != nil {
		return p, errors.New("profile not found")
	}
	e = json.Unmarshal([]byte(raw), &p)
	return p, e
}
func (s *Store) Profiles() ([]profile.Profile, error) {
	rows, e := s.DB.Query("SELECT config FROM profiles ORDER BY name")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []profile.Profile{}
	for rows.Next() {
		var raw string
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		var p profile.Profile
		if e = json.Unmarshal([]byte(raw), &p); e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) Start(name, mode string, lower, upper int64) (Run, error) {
	if mode != "full" && mode != "incremental" {
		return Run{}, errors.New("mode must be full or incremental")
	}
	tx, e := s.DB.Begin()
	if e != nil {
		return Run{}, e
	}
	defer tx.Rollback()
	var raw string
	var water sql.NullInt64
	if e = tx.QueryRow("SELECT config,watermark FROM profiles WHERE name=?", name).Scan(&raw, &water); e != nil {
		return Run{}, errors.New("profile not found")
	}
	var p profile.Profile
	if e = json.Unmarshal([]byte(raw), &p); e != nil {
		return Run{}, e
	}
	if mode == "incremental" {
		if !water.Valid {
			return Run{}, errors.New("complete a full run before incremental")
		}
		lower = water.Int64
	}
	chunks, e := profile.Partitions(lower, upper, p.ChunkSize)
	if e != nil {
		return Run{}, e
	}
	id := uuid.NewString()
	now := stamp()
	_, e = tx.Exec("INSERT INTO runs(id,profile,config,mode,status,lower_id,upper_id,created,updated) VALUES(?,?,?,?,'queued',?,?,?,?)", id, name, raw, mode, lower, upper, now, now)
	if e != nil {
		return Run{}, errors.New("profile already has an unfinished run; resume it")
	}
	for i, c := range chunks {
		if _, e = tx.Exec("INSERT INTO chunks(run_id,idx,lower_id,upper_id) VALUES(?,?,?,?)", id, i, c.Lower, c.Upper); e != nil {
			return Run{}, e
		}
	}
	if e = tx.Commit(); e != nil {
		return Run{}, e
	}
	return s.GetRun(id)
}

const runSelect = `SELECT r.id,r.profile,r.mode,r.status,r.lower_id,r.upper_id,COALESCE(MAX(CASE WHEN c.status='completed' THEN c.upper_id END),r.lower_id),COUNT(c.idx),COALESCE(SUM(c.status='completed'),0),COALESCE(SUM(CASE WHEN c.status='completed' THEN c.row_count ELSE 0 END),0),r.error_code,r.created,r.updated FROM runs r LEFT JOIN chunks c ON c.run_id=r.id`

func scanRun(row interface{ Scan(...any) error }) (Run, error) {
	var r Run
	e := row.Scan(&r.ID, &r.Profile, &r.Mode, &r.Status, &r.Lower, &r.Upper, &r.LastID, &r.Total, &r.Completed, &r.Rows, &r.Error, &r.Created, &r.Updated)
	return r, e
}
func (s *Store) GetRun(id string) (Run, error) {
	r, e := scanRun(s.DB.QueryRow(runSelect+" WHERE r.id=? GROUP BY r.id", id))
	if e != nil {
		return r, errors.New("run not found")
	}
	return r, nil
}
func (s *Store) Runs() ([]Run, error) {
	rows, e := s.DB.Query(runSelect + " GROUP BY r.id ORDER BY r.created DESC LIMIT 50")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Run{}
	for rows.Next() {
		r, e := scanRun(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) Pause(id string) error {
	res, e := s.DB.Exec("UPDATE runs SET status='paused',updated=? WHERE id=? AND status IN ('queued','running','paused')", stamp(), id)
	if e != nil {
		return e
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("run is not pausable")
	}
	return nil
}
func (s *Store) Resume(id string) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var status string
	if e = tx.QueryRow("SELECT status FROM runs WHERE id=?", id).Scan(&status); e != nil {
		return errors.New("run not found")
	}
	if status == "completed" {
		return errors.New("run is already complete")
	}
	if status == "failed" {
		if _, e = tx.Exec("UPDATE chunks SET status='pending' WHERE run_id=? AND status='failed'", id); e != nil {
			return e
		}
	}
	if _, e = tx.Exec("UPDATE runs SET status=CASE WHEN status='running' THEN status ELSE 'queued' END,error_code='',updated=? WHERE id=?", stamp(), id); e != nil {
		return e
	}
	return tx.Commit()
}

// Recover is called only by the process holding the exclusive worker lock.
func (s *Store) Recover() error {
	_, e := s.DB.Exec("UPDATE chunks SET status='pending' WHERE status='running'; UPDATE runs SET status='queued' WHERE status='running'")
	return e
}
func (s *Store) Next() (*Chunk, error) {
	tx, e := s.DB.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var c Chunk
	var raw string
	e = tx.QueryRow("SELECT c.run_id,c.idx,c.lower_id,c.upper_id,c.attempt,r.config FROM chunks c JOIN runs r ON r.id=c.run_id WHERE r.status IN ('queued','running') AND c.status='pending' ORDER BY r.created,c.idx LIMIT 1").Scan(&c.RunID, &c.Index, &c.Lower, &c.Upper, &c.Attempt, &raw)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal([]byte(raw), &c.Profile); e != nil {
		return nil, e
	}
	c.Attempt++
	if _, e = tx.Exec("UPDATE chunks SET status='running',attempt=? WHERE run_id=? AND idx=?", c.Attempt, c.RunID, c.Index); e != nil {
		return nil, e
	}
	if _, e = tx.Exec("UPDATE runs SET status='running',updated=? WHERE id=?", stamp(), c.RunID); e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return &c, nil
}
func (s *Store) Complete(c *Chunk, rows int64) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.Exec("UPDATE chunks SET status='completed',row_count=? WHERE run_id=? AND idx=?", rows, c.RunID, c.Index); e != nil {
		return e
	}
	var left int
	if e = tx.QueryRow("SELECT count(*) FROM chunks WHERE run_id=? AND status<>'completed'", c.RunID).Scan(&left); e != nil {
		return e
	}
	if left == 0 {
		if _, e = tx.Exec("UPDATE runs SET status='completed',error_code='',updated=? WHERE id=?", stamp(), c.RunID); e != nil {
			return e
		}
		if _, e = tx.Exec("UPDATE profiles SET watermark=MAX(COALESCE(watermark,0),(SELECT upper_id FROM runs WHERE id=?)) WHERE name=?", c.RunID, c.Profile.Name); e != nil {
			return e
		}
	} else {
		if _, e = tx.Exec("UPDATE runs SET updated=? WHERE id=?", stamp(), c.RunID); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) Fail(c *Chunk, code string) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.Exec("UPDATE chunks SET status='failed' WHERE run_id=? AND idx=?", c.RunID, c.Index); e != nil {
		return e
	}
	if _, e = tx.Exec("UPDATE runs SET status='failed',error_code=?,updated=? WHERE id=?", code, stamp(), c.RunID); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) StagePrefix(c *Chunk) string { return fmt.Sprintf("p_%s_%d_", c.RunID[:8], c.Index) }
