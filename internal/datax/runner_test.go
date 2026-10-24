package datax

import (
	"encoding/json"
	"github.com/LyleLiu666/datax-mcp/internal/profile"
	"strings"
	"testing"
)

func TestGeneratedJobIsBoundedAndDoesNotWriteFinalTable(t *testing.T) {
	p := profile.Profile{Reader: json.RawMessage(`{"name":"streamreader","parameter":{}}`), Columns: []string{"id", "body"}}
	raw, e := Job(p, p.Reader, "datax_mcp_stage.p_test", "postgresql://demo:secret@localhost:5432/test?sslmode=disable")
	if e != nil {
		t.Fatal(e)
	}
	var root map[string]any
	json.Unmarshal(raw, &root)
	job := root["job"].(map[string]any)
	setting := job["setting"].(map[string]any)
	if setting["errorLimit"].(map[string]any)["record"] != float64(0) {
		t.Fatal("dirty records accepted")
	}
	writer := job["content"].([]any)[0].(map[string]any)["writer"].(map[string]any)["parameter"].(map[string]any)
	if writer["emptyAsNull"] != false {
		t.Fatal("empty strings collapsed")
	}
	if writer["connection"].([]any)[0].(map[string]any)["table"].([]any)[0] != "datax_mcp_stage.p_test" {
		t.Fatal("not staging")
	}
	if _, ok := writer["writeMode"]; ok {
		t.Fatal("DataX PG writer does not support writeMode")
	}
}

func TestTargetTLSMustBeExplicitAndPreserved(t *testing.T) {
	p := profile.Profile{Columns: []string{"id"}}
	for _, mode := range []string{"disable", "require", "verify-ca", "verify-full"} {
		raw, err := Job(p, json.RawMessage(`{}`), "stage", "postgresql://demo:secret@localhost/test?sslmode="+mode)
		if err != nil || !strings.Contains(string(raw), "sslmode="+mode) {
			t.Fatalf("mode %s: %s %v", mode, raw, err)
		}
	}
	for _, dsn := range []string{"host=localhost password=secret", "postgresql://demo:secret@localhost/test", "postgresql://demo:secret@localhost/test?sslmode=prefer", "postgresql://demo@localhost/test?sslmode=disable"} {
		if _, err := Job(p, json.RawMessage(`{}`), "stage", dsn); err == nil {
			t.Fatal("ambiguous or empty-password connection accepted")
		}
	}
}
