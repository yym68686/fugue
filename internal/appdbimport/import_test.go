package appdbimport

import (
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestCommandOutputDrainsWithoutRetainingUnboundedLogs(t *testing.T) {
	var out commandOutput
	prefix := strings.Repeat("x", maxCommandOutput-1)
	if n, err := io.Copy(&out, strings.NewReader(prefix+"é"+strings.Repeat("y", 1<<20))); err != nil || n != int64(maxCommandOutput+1+(1<<20)) {
		t.Fatalf("drain returned %d, %v", n, err)
	}
	if got := out.String(); !utf8.ValidString(got) || got != prefix+"...[truncated]" {
		t.Fatalf("unexpected bounded output (bytes=%d, UTF-8=%v)", len(got), utf8.ValidString(got))
	}
}

func TestCommandOutputTrimsSmallLog(t *testing.T) {
	var out commandOutput
	_, _ = io.WriteString(&out, "\n NOTICE: done \n")
	if got := out.String(); got != "NOTICE: done" {
		t.Fatalf("small log = %q", got)
	}
}
