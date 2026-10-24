package profile

import (
	"encoding/json"
	"strings"
	"testing"
)

func exampleProfile() Profile {
	return Profile{Name: "demo", TargetTable: "public.demo", Key: "id", Columns: []string{"id", "content"}, TargetDSNEnv: "DEMO_PG_DSN", ChunkSize: 10, Reader: json.RawMessage(`{"name":"mysqlreader","parameter":{"username":"${MYSQL_USER}","password":"${MYSQL_PASSWORD}","connection":[{"jdbcUrl":["jdbc:mysql://localhost:3306/demo"],"querySql":["SELECT id, content FROM demo WHERE id > {{lower}} AND id <= {{upper}}"]}]}}`)}
}
func TestProfileRejectsInlineSecretsAndInvalidIdentifiers(t *testing.T) {
	p := exampleProfile()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	p.Reader = json.RawMessage(strings.Replace(string(p.Reader), "${MYSQL_PASSWORD}", "literal-secret", 1))
	if p.Validate() == nil {
		t.Fatal("inline secret accepted")
	}
	p = exampleProfile()
	p.TargetTable = "public.demo; DROP TABLE users"
	if p.Validate() == nil {
		t.Fatal("SQL identifier injection accepted")
	}
	p = exampleProfile()
	p.Columns = []string{"content"}
	if p.Validate() == nil {
		t.Fatal("missing key accepted")
	}
}
func TestRenderDoesNotRescanSecretValuesAsTemplates(t *testing.T) {
	p := exampleProfile()
	p.Reader = json.RawMessage(strings.Replace(string(p.Reader), "${MYSQL_USER}", "${MYSQL_USER}", 1))
	get := func(k string) (string, bool) {
		return map[string]string{"MYSQL_USER": "reader", "MYSQL_PASSWORD": "quote\"\\{{upper}} ${OTHER}\n"}[k], k == "MYSQL_USER" || k == "MYSQL_PASSWORD"
	}
	out, err := Render(p.Reader, Bounds{Lower: 10, Upper: 20}, get)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if json.Unmarshal(out, &v) != nil {
		t.Fatal("invalid JSON")
	}
	param := v["parameter"].(map[string]any)
	if param["password"] != "quote\"\\{{upper}} ${OTHER}\n" {
		t.Fatal("secret was rewritten")
	}
	if strings.Contains(string(out), "id > {{lower}}") {
		t.Fatal("bound not rendered")
	}
}
func TestRangePartitionHasNoOverlapAndHandlesLastShortChunk(t *testing.T) {
	ranges, err := Partitions(3, 26, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []Bounds{{3, 13}, {13, 23}, {23, 26}}
	if len(ranges) != len(want) {
		t.Fatal(ranges)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("got %v", ranges)
		}
	}
	for _, v := range [][3]int64{{10, 3, 1}, {0, 10, 0}, {-1, 10, 2}, {0, 1000000, 1}} {
		if _, err := Partitions(v[0], v[1], v[2]); err == nil {
			t.Fatal("unbounded/invalid partition accepted", v)
		}
	}
}
func TestMissingEnvironmentReportsNameOnly(t *testing.T) {
	// Only one variable is missing. Map traversal order must not decide which
	// error this test expects when multiple variables are absent.
	_, err := Render(exampleProfile().Reader, Bounds{0, 10}, func(key string) (string, bool) {
		if key == "MYSQL_USER" {
			return "synthetic-user", true
		}
		return "must-not-appear-in-diagnostics", false
	})
	if err == nil || err.Error() != "missing environment variable MYSQL_PASSWORD" {
		t.Fatal(err)
	}
}

func TestRenderPreservesLargeInteger(t *testing.T) {
	raw, err := Render(json.RawMessage(`{"n":9223372036854775807}`), Bounds{}, func(string) (string, bool) { return "", false })
	if err != nil || string(raw) != `{"n":9223372036854775807}` {
		t.Fatalf("%s %v", raw, err)
	}
}
