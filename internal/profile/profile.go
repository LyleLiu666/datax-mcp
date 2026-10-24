// Package profile describes an approved DataX reader and PostgreSQL publication target.
package profile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type Profile struct {
	Name           string          `json:"name" jsonschema:"Stable profile name; lowercase letters, digits, hyphens"`
	Description    string          `json:"description,omitempty"`
	Reader         json.RawMessage `json:"reader" jsonschema:"Native DataX reader configuration with environment placeholders and numeric range placeholders"`
	TargetTable    string          `json:"target_table" jsonschema:"Existing PostgreSQL target, schema.table"`
	TargetDSNEnv   string          `json:"target_dsn_env" jsonschema:"Name of environment variable containing the PostgreSQL connection string; never the secret itself"`
	Key            string          `json:"key" jsonschema:"Single integer primary or unique key"`
	Columns        []string        `json:"columns" jsonschema:"Ordered target columns corresponding to reader output"`
	ChunkSize      int64           `json:"chunk_size" jsonschema:"ID range width, not a promise about row count"`
	TimeoutSeconds int             `json:"timeout_seconds,omitempty"`
}
type Bounds struct {
	Lower int64 `json:"lower"`
	Upper int64 `json:"upper"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
var name = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var envName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
var token = regexp.MustCompile(`\$\{([A-Z_][A-Z0-9_]*)\}|\{\{(lower|upper|first)\}\}`)

func Ident(s string) bool { return identifier.MatchString(s) }
func TableParts(s string) (string, string, error) {
	p := strings.Split(s, ".")
	if len(p) != 2 || !Ident(p[0]) || !Ident(p[1]) {
		return "", "", errors.New("target_table must be schema.table identifiers")
	}
	if p[0] == "pg_catalog" || p[0] == "information_schema" || strings.HasPrefix(p[0], "datax_mcp_") {
		return "", "", errors.New("reserved target schema")
	}
	return p[0], p[1], nil
}
func (p Profile) Validate() error {
	if !name.MatchString(p.Name) {
		return errors.New("invalid profile name")
	}
	if _, _, err := TableParts(p.TargetTable); err != nil {
		return err
	}
	if !envName.MatchString(p.TargetDSNEnv) {
		return errors.New("target_dsn_env must name an environment variable")
	}
	if !Ident(p.Key) || len(p.Columns) == 0 || len(p.Columns) > 256 {
		return errors.New("invalid key or column count")
	}
	found := false
	seen := map[string]bool{}
	for _, c := range p.Columns {
		if !Ident(c) || seen[c] {
			return errors.New("invalid or duplicate column")
		}
		seen[c] = true
		found = found || c == p.Key
	}
	if !found {
		return errors.New("key must be among columns")
	}
	if p.ChunkSize < 1 {
		return errors.New("chunk_size must be positive")
	}
	if p.TimeoutSeconds != 0 && (p.TimeoutSeconds < 5 || p.TimeoutSeconds > 3600) {
		return errors.New("timeout_seconds must be 5..3600")
	}
	var reader map[string]any
	if json.Unmarshal(p.Reader, &reader) != nil || reader["name"] == nil || reader["parameter"] == nil {
		return errors.New("invalid DataX reader")
	}
	if reader["name"] != "mysqlreader" && reader["name"] != "streamreader" {
		return errors.New("first release supports mysqlreader and synthetic streamreader")
	}
	if reader["name"] == "mysqlreader" && (!bytes.Contains(p.Reader, []byte("{{lower}}")) || !bytes.Contains(p.Reader, []byte("{{upper}}"))) {
		return errors.New("MySQL reader must use both range bounds")
	}
	return validateSecrets(reader)
}
func validateSecrets(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for k, v := range x {
			if strings.EqualFold(k, "password") {
				s, ok := v.(string)
				if !ok || !strings.HasPrefix(s, "${") || !strings.HasSuffix(s, "}") || !envName.MatchString(s[2:len(s)-1]) {
					return errors.New("password must be an environment placeholder")
				}
			}
			if err := validateSecrets(v); err != nil {
				return err
			}
		}
	case []any:
		for _, v := range x {
			if err := validateSecrets(v); err != nil {
				return err
			}
		}
	}
	return nil
}

// Render walks JSON values, substituting each token once. Secret contents are
// never reparsed as a template and can safely contain quotes or placeholder text.
func Render(raw json.RawMessage, b Bounds, getenv func(string) (string, bool)) (json.RawMessage, error) {
	var v any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&v); err != nil {
		return nil, errors.New("invalid reader JSON")
	}
	var walk func(any) (any, error)
	walk = func(v any) (any, error) {
		switch x := v.(type) {
		case string:
			var failure error
			out := token.ReplaceAllStringFunc(x, func(t string) string {
				if strings.HasPrefix(t, "${") {
					key := t[2 : len(t)-1]
					value, ok := getenv(key)
					if !ok {
						failure = fmt.Errorf("missing environment variable %s", key)
					}
					return value
				}
				switch t {
				case "{{lower}}":
					return fmt.Sprint(b.Lower)
				case "{{upper}}":
					return fmt.Sprint(b.Upper)
				default:
					return fmt.Sprint(b.Lower + 1)
				}
			})
			return out, failure
		case map[string]any:
			for k, v := range x {
				out, err := walk(v)
				if err != nil {
					return nil, err
				}
				x[k] = out
			}
			return x, nil
		case []any:
			for i, v := range x {
				out, err := walk(v)
				if err != nil {
					return nil, err
				}
				x[i] = out
			}
			return x, nil
		default:
			return v, nil
		}
	}
	out, err := walk(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}
func Partitions(lower, upper, size int64) ([]Bounds, error) {
	if lower < 0 || upper <= lower || size <= 0 {
		return nil, errors.New("require 0 <= lower < upper and positive chunk size")
	}
	n := (upper-lower-1)/size + 1
	if n > 10000 {
		return nil, errors.New("at most 10000 partitions per run; increase chunk size")
	}
	out := make([]Bounds, 0, n)
	for lower < upper {
		next := upper
		if upper-lower > size {
			next = lower + size
		}
		out = append(out, Bounds{lower, next})
		lower = next
	}
	return out, nil
}
