package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The SDK's typed default-application path remarshal uses float64, which rounds
// bigint cursors. Keep the official protocol transport and schemas, but decode
// requests directly into their concrete Go types without that conversion.
func addExactTool[In, Out any](s *mcp.Server, t *mcp.Tool, h func(context.Context, *mcp.CallToolRequest, In) (*mcp.CallToolResult, Out, error)) {
	if t.InputSchema == nil {
		schema, e := jsonschema.For[In](nil)
		if e != nil {
			panic(e)
		}
		t.InputSchema = schema
	}
	if t.OutputSchema == nil {
		schema, e := jsonschema.For[Out](nil)
		if e != nil {
			panic(e)
		}
		t.OutputSchema = schema
	}
	s.AddTool(t, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		failure := func(err error) (*mcp.CallToolResult, error) {
			r := new(mcp.CallToolResult)
			r.SetError(err)
			return r, nil
		}
		raw := bytes.TrimSpace(req.Params.Arguments)
		if len(raw) == 0 {
			raw = []byte(`{}`)
		}
		if raw[0] != '{' {
			return failure(errors.New("arguments must be an object"))
		}
		var in In
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if e := decoder.Decode(&in); e != nil {
			return failure(errors.New("invalid tool argument types or fields"))
		}
		var trailing any
		if decoder.Decode(&trailing) != io.EOF {
			return failure(errors.New("invalid trailing arguments"))
		}
		result, out, e := h(ctx, req, in)
		if e != nil {
			return failure(e)
		}
		encoded, e := json.Marshal(out)
		if e != nil {
			return failure(errors.New("cannot encode tool result"))
		}
		if result == nil {
			result = new(mcp.CallToolResult)
		}
		result.StructuredContent = json.RawMessage(encoded)
		result.Content = []mcp.Content{&mcp.TextContent{Text: string(encoded)}}
		return result, nil
	})
}
