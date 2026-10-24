// Package datax launches the upstream Java engine. It never transports rows in Go.
package datax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"github.com/jackc/pgx/v5"
)

var ErrFailed = errors.New("datax_failed")
var ErrTimeout = errors.New("datax_timeout")

type Runner struct {
	Home string
	Java string
}

func Job(p profile.Profile, reader json.RawMessage, stage, dsn string) ([]byte, error) {
	// Explicit URL options keep JDBC and pgx transport security aligned.
	source, e := url.Parse(dsn)
	if e != nil || (source.Scheme != "postgresql" && source.Scheme != "postgres") || source.Host == "" {
		return nil, errors.New("target connection must be a PostgreSQL URL")
	}
	query := source.Query()
	switch query.Get("sslmode") {
	case "disable", "require", "verify-ca", "verify-full":
	default:
		return nil, errors.New("target URL requires explicit sslmode: disable, require, verify-ca, or verify-full")
	}
	for k := range query {
		switch k {
		case "sslmode", "sslrootcert", "sslcert", "sslkey":
		default:
			return nil, errors.New("unsupported target URL option")
		}
	}
	c, e := pgx.ParseConfig(dsn)
	if e != nil {
		return nil, errors.New("invalid target connection")
	}
	if c.Password == "" {
		return nil, errors.New("DataX requires a nonempty target password")
	}
	u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(c.Host, fmt.Sprint(c.Port)), Path: "/" + c.Database, RawQuery: query.Encode()}
	writer := map[string]any{"name": "postgresqlwriter", "parameter": map[string]any{
		"username": c.User, "password": c.Password, "column": p.Columns, "batchSize": 500, "batchByteSize": 1048576, "emptyAsNull": false,
		"connection": []any{map[string]any{"jdbcUrl": "jdbc:" + u.String(), "table": []string{stage}}},
	}}
	root := map[string]any{"core": map[string]any{"container": map[string]any{"job": map[string]any{"sleepInterval": 1000}}, "transport": map[string]any{"channel": map[string]any{"speed": map[string]any{"byte": 1048576, "record": -1}}}}, "job": map[string]any{
		"setting": map[string]any{"speed": map[string]any{"channel": 1, "byte": 1048576}, "errorLimit": map[string]any{"record": 0, "percentage": 0}},
		"content": []any{map[string]any{"reader": reader, "writer": writer}},
	}}
	return json.Marshal(root)
}
func (r Runner) Check() error {
	for _, p := range []string{"conf/core.json", "lib", "plugin/writer/postgresqlwriter"} {
		if _, e := os.Stat(filepath.Join(r.Home, p)); e != nil {
			return errors.New("DataX runtime is incomplete; set DATAX_HOME")
		}
	}
	java := r.Java
	if java == "" {
		java = "java"
	}
	if _, e := exec.LookPath(java); e != nil {
		return errors.New("Java executable not found")
	}
	return nil
}
func (r Runner) Load(ctx context.Context, p profile.Profile, b profile.Bounds, stage, dsn, dir string) error {
	if e := r.Check(); e != nil {
		return e
	}
	reader, e := profile.Render(p.Reader, b, os.LookupEnv)
	if e != nil {
		return e
	}
	job, e := Job(p, reader, stage, dsn)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return errors.New("cannot create private job directory")
	}
	config := filepath.Join(dir, "job.json")
	if e = os.WriteFile(config, job, 0600); e != nil {
		return errors.New("cannot write private DataX config")
	}
	defer os.Remove(config)
	// Replace upstream file appenders, which otherwise write under DATAX_HOME.
	// All raw diagnostics stay in this private attempt directory, never MCP output.
	logConfig := filepath.Join(dir, "logback.xml")
	logging := `<configuration><appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender"><encoder><pattern>%level %logger - %msg%n</pattern></encoder></appender><root level="WARN"><appender-ref ref="CONSOLE"/></root></configuration>`
	if e = os.WriteFile(logConfig, []byte(logging), 0600); e != nil {
		return errors.New("cannot write logging config")
	}
	log, e := os.OpenFile(filepath.Join(dir, "datax.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if e != nil {
		return errors.New("cannot open private log")
	}
	defer log.Close()
	seconds := p.TimeoutSeconds
	if seconds == 0 {
		seconds = 120
	}
	deadline, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	defer cancel()
	java := r.Java
	if java == "" {
		java = "java"
	}
	cmd := exec.CommandContext(deadline, java, "-Xms128m", "-Xmx512m", "-Dfile.encoding=UTF-8", "-Ddatax.home="+r.Home, "-Dlogback.configurationFile="+logConfig, "-cp", filepath.Join(r.Home, "lib", "*"), "com.alibaba.datax.core.Engine", "-mode", "standalone", "-jobid", "1", "-job", config)
	cmd.Dir = dir
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 5 * time.Second
	if e = cmd.Run(); e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if deadline.Err() != nil {
			return ErrTimeout
		}
		return ErrFailed
	}
	return nil
}
