package releaseadapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"fugue/internal/releasedomain"
)

const (
	TraceAPIVersion = "release-transaction-trace.fugue.dev/v1"
	TraceKind       = "ReleaseTransactionTraceEvent"

	maxTraceEvents = 64
	maxTraceBytes  = 8 << 10
)

// TracePhase includes the transaction envelope around the four adapter
// commands. It is intentionally distinct from Phase so evidence cannot imply
// that a transaction record is an executable adapter command.
type TracePhase string

const (
	TracePhaseTransaction TracePhase = "transaction"
	TracePhasePrepare     TracePhase = "prepare"
	TracePhaseApply       TracePhase = "apply"
	TracePhaseVerify      TracePhase = "verify"
	TracePhaseRollback    TracePhase = "rollback"
)

// TraceState is one fixed, secret-free state transition.
type TraceState string

const (
	TraceStateStarted   TraceState = "started"
	TraceStateSucceeded TraceState = "succeeded"
	TraceStateFailed    TraceState = "failed"
)

// TraceEvent contains only bounded identifiers and fixed enum values. Command
// argv, output, errors, manifests, and environment values are never included.
type TraceEvent struct {
	APIVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Sequence   uint64               `json:"sequence"`
	Domain     releasedomain.Domain `json:"domain"`
	PlanDigest string               `json:"planDigest"`
	Phase      TracePhase           `json:"phase"`
	State      TraceState           `json:"state"`
}

// Trace durably records transaction state. Record must not retain mutable
// references to the supplied event. Barrier is a non-closing durability check:
// it must leave the sink able to record rollback evidence if the barrier or
// the final pre-commit context check fails.
type Trace interface {
	Record(TraceEvent) error
	Barrier() error
}

func newTraceEvent(
	sequence uint64,
	authorization interface {
		Domain() releasedomain.Domain
		PlanDigest() string
	},
	phase TracePhase,
	state TraceState,
) TraceEvent {
	return TraceEvent{
		APIVersion: TraceAPIVersion,
		Kind:       TraceKind,
		Sequence:   sequence,
		Domain:     authorization.Domain(),
		PlanDigest: authorization.PlanDigest(),
		Phase:      phase,
		State:      state,
	}
}

// PrivateFileTrace is a synchronized JSONL trace in a freshly created 0700
// directory. Every successfully recorded line is flushed with fsync before
// Record returns.
type PrivateFileTrace struct {
	mutex         sync.Mutex
	file          *os.File
	directoryPath string
	filePath      string
	directoryInfo os.FileInfo
	fileInfo      os.FileInfo
	nextSequence  uint64
	eventCount    int
	byteCount     int
	broken        error
	closed        bool
	terminalErr   error
}

// NewPrivateFileTrace creates an isolated trace below parentDirectory. The
// parent is resolved once; the returned directory and file are never symlinks.
func NewPrivateFileTrace(parentDirectory string) (*PrivateFileTrace, error) {
	if strings.TrimSpace(parentDirectory) == "" {
		return nil, fmt.Errorf("private trace parent directory is empty")
	}
	absoluteParent, err := filepath.Abs(parentDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve private trace parent: %w", err)
	}
	resolvedParent, err := filepath.EvalSymlinks(absoluteParent)
	if err != nil {
		return nil, fmt.Errorf("resolve private trace parent: %w", err)
	}
	parentInfo, err := os.Stat(resolvedParent)
	if err != nil {
		return nil, fmt.Errorf("inspect private trace parent: %w", err)
	}
	if !parentInfo.IsDir() {
		return nil, fmt.Errorf("private trace parent is not a directory")
	}

	directoryPath, err := os.MkdirTemp(resolvedParent, "fugue-release-transaction-")
	if err != nil {
		return nil, fmt.Errorf("create private trace directory: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(directoryPath)
		}
	}()
	if err := os.Chmod(directoryPath, 0o700); err != nil {
		return nil, fmt.Errorf("set private trace directory mode: %w", err)
	}
	directoryInfo, err := os.Lstat(directoryPath)
	if err != nil || directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() || directoryInfo.Mode().Perm() != 0o700 {
		return nil, fmt.Errorf("private trace directory is not an exact 0700 non-symlink directory")
	}
	currentParentInfo, err := os.Stat(resolvedParent)
	if err != nil || !os.SameFile(parentInfo, currentParentInfo) {
		return nil, fmt.Errorf("private trace parent identity changed during creation")
	}

	filePath := filepath.Join(directoryPath, "transaction.jsonl")
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create private trace file: %w", err)
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("set private trace file mode: %w", err)
	}
	fileInfo, err := file.Stat()
	if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("private trace file is not an exact 0600 regular file")
	}
	pathInfo, err := os.Lstat(filePath)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(fileInfo, pathInfo) {
		return nil, fmt.Errorf("private trace file path does not identify the opened file")
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync private trace file: %w", err)
	}

	cleanup = false
	closeFile = false
	return &PrivateFileTrace{
		file:          file,
		directoryPath: directoryPath,
		filePath:      filePath,
		directoryInfo: directoryInfo,
		fileInfo:      fileInfo,
		nextSequence:  1,
	}, nil
}

// DirectoryPath returns the isolated evidence directory.
func (trace *PrivateFileTrace) DirectoryPath() string {
	if trace == nil {
		return ""
	}
	return trace.directoryPath
}

// FilePath returns the JSONL evidence path.
func (trace *PrivateFileTrace) FilePath() string {
	if trace == nil {
		return ""
	}
	return trace.filePath
}

// Record implements Trace.
func (trace *PrivateFileTrace) Record(event TraceEvent) error {
	if trace == nil {
		return fmt.Errorf("private trace is nil")
	}
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	if trace.broken != nil {
		return trace.broken
	}
	if trace.closed {
		return fmt.Errorf("private trace is closed")
	}
	if err := validateTraceEvent(event, trace.nextSequence); err != nil {
		return err
	}
	if trace.eventCount >= maxTraceEvents {
		return fmt.Errorf("private trace exceeds %d-event limit", maxTraceEvents)
	}
	if err := trace.verifyPathIdentity(); err != nil {
		trace.broken = err
		return err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode private trace event: %w", err)
	}
	encoded = append(encoded, '\n')
	if trace.byteCount+len(encoded) > maxTraceBytes {
		return fmt.Errorf("private trace exceeds %d-byte limit", maxTraceBytes)
	}
	if err := writeFull(trace.file, encoded); err != nil {
		trace.broken = fmt.Errorf("write private trace event: %w", err)
		return trace.broken
	}
	if err := trace.file.Sync(); err != nil {
		trace.broken = fmt.Errorf("sync private trace event: %w", err)
		return trace.broken
	}
	if err := trace.verifyPathIdentity(); err != nil {
		trace.broken = err
		return err
	}
	trace.nextSequence++
	trace.eventCount++
	trace.byteCount += len(encoded)
	return nil
}

func (trace *PrivateFileTrace) verifyPathIdentity() error {
	directoryInfo, err := os.Lstat(trace.directoryPath)
	if err != nil || directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() || directoryInfo.Mode().Perm() != 0o700 || !os.SameFile(trace.directoryInfo, directoryInfo) {
		return fmt.Errorf("private trace directory identity changed")
	}
	pathInfo, err := os.Lstat(trace.filePath)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() || pathInfo.Mode().Perm() != 0o600 || !os.SameFile(trace.fileInfo, pathInfo) {
		return fmt.Errorf("private trace file identity changed")
	}
	openedInfo, err := trace.file.Stat()
	if err != nil || openedInfo.Mode().Perm() != 0o600 || !os.SameFile(trace.fileInfo, openedInfo) {
		return fmt.Errorf("opened private trace file identity changed")
	}
	return nil
}

func (trace *PrivateFileTrace) verifyClosedPathIdentity() error {
	directoryInfo, err := os.Lstat(trace.directoryPath)
	if err != nil || directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() || directoryInfo.Mode().Perm() != 0o700 || !os.SameFile(trace.directoryInfo, directoryInfo) {
		return fmt.Errorf("private trace directory identity changed after close")
	}
	pathInfo, err := os.Lstat(trace.filePath)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() || pathInfo.Mode().Perm() != 0o600 || !os.SameFile(trace.fileInfo, pathInfo) {
		return fmt.Errorf("private trace file identity changed after close")
	}
	return nil
}

// Barrier implements Trace with a non-closing durability and identity check.
// Keeping the file open is essential: if the pre-commit barrier fails after
// Apply, the runner must still be able to record its exactly-once rollback.
func (trace *PrivateFileTrace) Barrier() error {
	if trace == nil {
		return fmt.Errorf("private trace is nil")
	}
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	if trace.broken != nil {
		return trace.broken
	}
	if trace.closed {
		if trace.terminalErr != nil {
			return trace.terminalErr
		}
		return fmt.Errorf("private trace is closed")
	}
	if err := trace.verifyPathIdentity(); err != nil {
		trace.broken = err
		return err
	}
	if err := trace.file.Sync(); err != nil {
		trace.broken = fmt.Errorf("sync private trace barrier: %w", err)
		return trace.broken
	}
	if err := trace.verifyPathIdentity(); err != nil {
		trace.broken = err
		return err
	}
	return nil
}

// Close flushes and closes the trace without removing its evidence directory.
func (trace *PrivateFileTrace) Close() error {
	if trace == nil {
		return fmt.Errorf("private trace is nil")
	}
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	if trace.closed {
		if trace.terminalErr == nil {
			trace.terminalErr = trace.verifyClosedPathIdentity()
		}
		return trace.terminalErr
	}
	trace.closed = true
	terminalErrors := make([]error, 0, 6)
	if trace.broken != nil {
		terminalErrors = append(terminalErrors, trace.broken)
	}
	if err := trace.verifyPathIdentity(); err != nil {
		terminalErrors = append(terminalErrors, err)
	}
	syncErr := trace.file.Sync()
	if syncErr != nil {
		terminalErrors = append(terminalErrors, fmt.Errorf("sync private trace on close: %w", syncErr))
	}
	if err := trace.verifyPathIdentity(); err != nil {
		terminalErrors = append(terminalErrors, err)
	}
	closeErr := trace.file.Close()
	if closeErr != nil {
		terminalErrors = append(terminalErrors, fmt.Errorf("close private trace: %w", closeErr))
	}
	if err := trace.verifyClosedPathIdentity(); err != nil {
		terminalErrors = append(terminalErrors, err)
	}
	trace.terminalErr = errors.Join(terminalErrors...)
	return trace.terminalErr
}

func validateTraceEvent(event TraceEvent, expectedSequence uint64) error {
	if event.APIVersion != TraceAPIVersion || event.Kind != TraceKind {
		return fmt.Errorf("private trace event identity is invalid")
	}
	if event.Sequence != expectedSequence {
		return fmt.Errorf("private trace event sequence %d, want %d", event.Sequence, expectedSequence)
	}
	if _, err := releasedomain.ParseDomain(string(event.Domain)); err != nil {
		return fmt.Errorf("private trace event: %w", err)
	}
	if !validLowerSHA256(event.PlanDigest) {
		return fmt.Errorf("private trace event plan digest is not lowercase sha256")
	}
	switch event.Phase {
	case TracePhaseTransaction, TracePhasePrepare, TracePhaseApply, TracePhaseVerify, TracePhaseRollback:
	default:
		return fmt.Errorf("private trace event phase %q is invalid", event.Phase)
	}
	switch event.State {
	case TraceStateStarted, TraceStateSucceeded, TraceStateFailed:
	default:
		return fmt.Errorf("private trace event state %q is invalid", event.State)
	}
	return nil
}

func validLowerSHA256(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if written > 0 {
			data = data[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
