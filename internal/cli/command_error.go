package cli

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

type payloadWriter struct {
	io.Writer
	written int
}

func (w *payloadWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.written += n
	return n, err
}

type commandErrorDescription struct {
	Code      string `json:"code"`
	Category  string `json:"category"`
	Message   string `json:"message"`
	ExitCode  int    `json:"exit_code"`
	Retryable bool   `json:"retryable"`
	RequestID string `json:"request_id,omitempty"`
}

func describeCommandError(err error) commandErrorDescription {
	code := ExitCodeForError(err)
	categories := map[int]string{2: "user_input", 3: "permission_denied", 4: "not_found", 5: "system_fault", 6: "indeterminate"}
	result := commandErrorDescription{Code: categories[code], Category: categories[code], Message: redactDiagnosticString(err.Error()), ExitCode: code}
	var api *apiServerError
	if errors.As(err, &api) {
		if api.Response.Code != "" {
			result.Code = api.Response.Code
		}
		result.Retryable = api.Response.Retryable || api.StatusCode == http.StatusTooManyRequests || api.StatusCode >= 500
		result.RequestID = api.Headers.Get("X-Request-ID")
	}
	return result
}
func (c *CLI) renderCommandError(err error) error {
	if outputErr := c.writeJSON(map[string]any{"schema_version": 1, "outcome": "unknown", "error": describeCommandError(err)}); outputErr != nil {
		return fmt.Errorf("%w; write error result: %v", err, outputErr)
	}
	return err
}

func requestedJSON(args []string) bool {
	want := false
	for i, arg := range args {
		if arg == "--" {
			break
		}
		switch arg {
		case "--json", "--json=true", "--output=json", "-o=json":
			want = true
		case "--json=false":
			want = false
		case "--output", "-o":
			if i+1 < len(args) {
				want = args[i+1] == "json"
			}
		}
	}
	return want
}
