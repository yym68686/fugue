package releaseadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/releasedomain"
)

func TestPrivateFileTracePermissionsAndSuccessfulTransaction(t *testing.T) {
	parent := t.TempDir()
	trace, err := NewPrivateFileTrace(parent)
	if err != nil {
		t.Fatalf("NewPrivateFileTrace() error = %v", err)
	}
	directoryInfo, err := os.Lstat(trace.DirectoryPath())
	if err != nil {
		t.Fatalf("lstat trace directory: %v", err)
	}
	if directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() || directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("trace directory mode = %v", directoryInfo.Mode())
	}
	fileInfo, err := os.Lstat(trace.FilePath())
	if err != nil {
		t.Fatalf("lstat trace file: %v", err)
	}
	if !fileInfo.Mode().IsRegular() || fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("trace file mode = %v", fileInfo.Mode())
	}

	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	log := &phaseLog{}
	if err := Run(
		context.Background(),
		time.Second,
		authorization,
		completeFakeAdapter(domain, log, nil, nil),
		trace,
	); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := trace.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.Open(trace.FilePath())
	if err != nil {
		t.Fatalf("open trace: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	events := make([]TraceEvent, 0, 8)
	for scanner.Scan() {
		var event TraceEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("decode trace event: %v", err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan trace: %v", err)
	}
	if len(events) != 8 {
		t.Fatalf("event count = %d, want 8", len(events))
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) || event.Domain != domain || event.PlanDigest != authorization.PlanDigest() {
			t.Fatalf("event[%d] = %#v", index, event)
		}
	}
	data, err := os.ReadFile(trace.FilePath())
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	for _, forbidden := range []string{"private", "argv", "manifest", "environment", "error"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("trace contains forbidden field/value %q: %s", forbidden, data)
		}
	}
	if err := trace.Record(events[0]); err == nil {
		t.Fatal("closed trace accepted an event")
	}
}

func TestPrivateFileTraceRejectsInvalidEventsWithoutAdvancingSequence(t *testing.T) {
	trace, err := NewPrivateFileTrace(t.TempDir())
	if err != nil {
		t.Fatalf("NewPrivateFileTrace() error = %v", err)
	}
	defer trace.Close()
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	valid := newTraceEvent(1, authorization, TracePhaseTransaction, TraceStateStarted)
	tests := []TraceEvent{
		func() TraceEvent { value := valid; value.APIVersion = "other/v1"; return value }(),
		func() TraceEvent { value := valid; value.Kind = "Other"; return value }(),
		func() TraceEvent { value := valid; value.Sequence = 2; return value }(),
		func() TraceEvent { value := valid; value.Domain = "other"; return value }(),
		func() TraceEvent { value := valid; value.PlanDigest = "sha256:UPPER"; return value }(),
		func() TraceEvent { value := valid; value.Phase = "other"; return value }(),
		func() TraceEvent { value := valid; value.State = "other"; return value }(),
	}
	for index, event := range tests {
		if err := trace.Record(event); err == nil {
			t.Fatalf("invalid event %d succeeded: %#v", index, event)
		}
	}
	if err := trace.Record(valid); err != nil {
		t.Fatalf("valid event after rejected inputs: %v", err)
	}
	data, err := os.ReadFile(trace.FilePath())
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	if lines := strings.Count(string(data), "\n"); lines != 1 {
		t.Fatalf("trace line count = %d, want 1: %s", lines, data)
	}
}

func TestPrivateFileTraceBarrierIsDurableAndNonClosing(t *testing.T) {
	trace, err := NewPrivateFileTrace(t.TempDir())
	if err != nil {
		t.Fatalf("NewPrivateFileTrace() error = %v", err)
	}
	authorization := testAuthorization(t, releasedomain.DomainNodeLocal)
	if err := trace.Record(newTraceEvent(1, authorization, TracePhaseTransaction, TraceStateStarted)); err != nil {
		t.Fatalf("Record(first) error = %v", err)
	}
	if err := trace.Barrier(); err != nil {
		t.Fatalf("Barrier() error = %v", err)
	}
	if err := trace.Record(newTraceEvent(2, authorization, TracePhasePrepare, TraceStateStarted)); err != nil {
		t.Fatalf("Record(after barrier) error = %v", err)
	}
	if err := trace.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	data, err := os.ReadFile(trace.FilePath())
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	if lines := strings.Count(string(data), "\n"); lines != 2 {
		t.Fatalf("trace line count = %d, want 2: %s", lines, data)
	}
}

func TestNewPrivateFileTraceResolvesParentAndFailsClosed(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatalf("mkdir real parent: %v", err)
	}
	symlinkParent := filepath.Join(root, "alias")
	if err := os.Symlink(realParent, symlinkParent); err != nil {
		t.Fatalf("symlink parent: %v", err)
	}
	trace, err := NewPrivateFileTrace(symlinkParent)
	if err != nil {
		t.Fatalf("NewPrivateFileTrace(symlink parent): %v", err)
	}
	defer trace.Close()
	resolvedRealParent, err := filepath.EvalSymlinks(realParent)
	if err != nil {
		t.Fatalf("resolve real parent: %v", err)
	}
	if !strings.HasPrefix(trace.DirectoryPath(), resolvedRealParent+string(os.PathSeparator)) {
		t.Fatalf("trace directory %q is not below resolved parent %q", trace.DirectoryPath(), resolvedRealParent)
	}

	regularFile := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(regularFile, []byte("fixture"), 0o600); err != nil {
		t.Fatalf("write regular parent fixture: %v", err)
	}
	for _, parent := range []string{"", filepath.Join(root, "missing"), regularFile} {
		if trace, err := NewPrivateFileTrace(parent); err == nil || trace != nil {
			t.Fatalf("NewPrivateFileTrace(%q) = %#v, %v", parent, trace, err)
		}
	}
}

func TestPrivateFileTraceFailsClosedWhenEvidencePathIsReplaced(t *testing.T) {
	trace, err := NewPrivateFileTrace(t.TempDir())
	if err != nil {
		t.Fatalf("NewPrivateFileTrace() error = %v", err)
	}
	defer trace.Close()
	originalPath := trace.FilePath() + ".original"
	if err := os.Rename(trace.FilePath(), originalPath); err != nil {
		t.Fatalf("rename trace fixture: %v", err)
	}
	if err := os.WriteFile(trace.FilePath(), []byte("replacement\n"), 0o600); err != nil {
		t.Fatalf("write replacement trace fixture: %v", err)
	}
	authorization := testAuthorization(t, releasedomain.DomainNodeLocal)
	event := newTraceEvent(1, authorization, TracePhaseTransaction, TraceStateStarted)
	if err := trace.Record(event); err == nil {
		t.Fatal("replaced evidence path accepted a trace event")
	}
	data, err := os.ReadFile(trace.FilePath())
	if err != nil {
		t.Fatalf("read replacement trace: %v", err)
	}
	if string(data) != "replacement\n" {
		t.Fatalf("replacement trace was modified: %q", data)
	}
}

func TestPrivateFileTraceCloseRevalidatesPathAndKeepsTerminalError(t *testing.T) {
	trace, err := NewPrivateFileTrace(t.TempDir())
	if err != nil {
		t.Fatalf("NewPrivateFileTrace() error = %v", err)
	}
	authorization := testAuthorization(t, releasedomain.DomainNodeLocal)
	if err := trace.Record(newTraceEvent(1, authorization, TracePhaseTransaction, TraceStateStarted)); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	originalPath := trace.FilePath() + ".original"
	if err := os.Rename(trace.FilePath(), originalPath); err != nil {
		t.Fatalf("rename trace fixture: %v", err)
	}
	if err := os.WriteFile(trace.FilePath(), []byte("replacement\n"), 0o600); err != nil {
		t.Fatalf("write replacement trace fixture: %v", err)
	}
	firstErr := trace.Close()
	if firstErr == nil {
		t.Fatal("Close() accepted a replaced evidence path")
	}
	secondErr := trace.Close()
	if secondErr == nil || secondErr.Error() != firstErr.Error() {
		t.Fatalf("second Close() error = %v, want sticky %v", secondErr, firstErr)
	}
	data, err := os.ReadFile(trace.FilePath())
	if err != nil {
		t.Fatalf("read replacement trace: %v", err)
	}
	if string(data) != "replacement\n" {
		t.Fatalf("replacement trace was modified: %q", data)
	}
}

func TestPrivateFileTraceContinuouslyEnforcesExactModes(t *testing.T) {
	authorization := testAuthorization(t, releasedomain.DomainNodeLocal)
	tests := []struct {
		name   string
		mutate func(*PrivateFileTrace) error
	}{
		{name: "directory mode", mutate: func(trace *PrivateFileTrace) error { return os.Chmod(trace.DirectoryPath(), 0o755) }},
		{name: "file mode", mutate: func(trace *PrivateFileTrace) error { return os.Chmod(trace.FilePath(), 0o644) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace, err := NewPrivateFileTrace(t.TempDir())
			if err != nil {
				t.Fatalf("NewPrivateFileTrace() error = %v", err)
			}
			if err := test.mutate(trace); err != nil {
				t.Fatalf("mutate trace mode: %v", err)
			}
			event := newTraceEvent(1, authorization, TracePhaseTransaction, TraceStateStarted)
			if err := trace.Record(event); err == nil {
				t.Fatal("Record() accepted widened evidence permissions")
			}
			firstErr := trace.Close()
			if firstErr == nil {
				t.Fatal("Close() accepted widened evidence permissions")
			}
			if secondErr := trace.Close(); secondErr == nil || secondErr.Error() != firstErr.Error() {
				t.Fatalf("second Close() error = %v, want sticky %v", secondErr, firstErr)
			}
		})
	}
}

func TestPrivateFileTraceEventAndByteLimits(t *testing.T) {
	authorization := testAuthorization(t, releasedomain.DomainNodeLocal)
	t.Run("event limit", func(t *testing.T) {
		trace, err := NewPrivateFileTrace(t.TempDir())
		if err != nil {
			t.Fatalf("NewPrivateFileTrace() error = %v", err)
		}
		// Use a larger byte budget only for this structural event-limit test.
		trace.byteCount = maxTraceBytes - 1
		event := newTraceEvent(1, authorization, TracePhaseTransaction, TraceStateStarted)
		if err := trace.Record(event); err == nil || !strings.Contains(err.Error(), "byte") {
			t.Fatalf("Record() byte-limit error = %v", err)
		}
		trace.byteCount = 0
		trace.eventCount = maxTraceEvents
		if err := trace.Record(event); err == nil || !strings.Contains(err.Error(), "event") {
			t.Fatalf("Record() event-limit error = %v", err)
		}
		if err := trace.Close(); err != nil {
			t.Fatalf("Close() after rejected limit events = %v", err)
		}
	})
}

func TestPrivateFileTraceConcurrentRecordAndCloseIsSerialized(t *testing.T) {
	authorization := testAuthorization(t, releasedomain.DomainNodeLocal)
	for iteration := 0; iteration < 100; iteration++ {
		trace, err := NewPrivateFileTrace(t.TempDir())
		if err != nil {
			t.Fatalf("NewPrivateFileTrace() error = %v", err)
		}
		start := make(chan struct{})
		recordResult := make(chan error, 1)
		closeResult := make(chan error, 1)
		go func() {
			<-start
			recordResult <- trace.Record(newTraceEvent(1, authorization, TracePhaseTransaction, TraceStateStarted))
		}()
		go func() {
			<-start
			closeResult <- trace.Close()
		}()
		close(start)
		recordErr := <-recordResult
		closeErr := <-closeResult
		if closeErr != nil {
			t.Fatalf("iteration %d Close() error = %v", iteration, closeErr)
		}
		if recordErr == nil {
			data, err := os.ReadFile(trace.FilePath())
			if err != nil {
				t.Fatalf("iteration %d read trace: %v", iteration, err)
			}
			if strings.Count(string(data), "\n") != 1 {
				t.Fatalf("iteration %d successful Record wrote invalid trace: %q", iteration, data)
			}
		}
	}
}
