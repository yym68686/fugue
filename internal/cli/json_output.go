package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

// Keep payload shapes and exact integer values while protecting embedded app
// specs even in callers that only intend to print policy or status metadata.
// Explicit secret viewing remains an opt-in; env/file payloads are untouched.
func (c *CLI) writeJSON(value any) error {
	if c.jsonSchema != "" {
		value = map[string]any{"schema_version": c.jsonSchema, "data": value}
	}
	if c.rawAppOutput {
		return writeJSON(c.stdout, value)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return err
	}
	return writeJSON(c.stdout, redactEmbeddedAppState(payload))
}
func redactEmbeddedAppState(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for k, child := range v {
			switch k {
			case "app", "apps", "operation", "operations", "source", "spec", "desired_spec", "origin_source", "build_source", "desired_source", "desired_origin_source":
				if spec, ok := child.(map[string]any); ok && (spec["template"] != nil || spec["containers"] != nil || spec["initContainers"] != nil) {
					v[k] = redactEmbeddedAppState(child)
				} else {
					v[k] = redactDiagnosticJSONValue(child, false)
				}
			default:
				v[k] = redactEmbeddedAppState(child)
			}
		}
	case []any:
		for i := range v {
			v[i] = redactEmbeddedAppState(v[i])
		}
	}
	return value
}
func terminalFile(w io.Writer) (*os.File, bool) {
	if p, ok := w.(*payloadWriter); ok {
		return terminalFile(p.Writer)
	}
	f, ok := w.(*os.File)
	return f, ok
}
