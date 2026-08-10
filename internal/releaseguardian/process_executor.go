package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"fugue/internal/declarativerelease"
)

type ProcessExecutor struct {
	Binary string
	PodUID string
}

func NewProcessExecutor(binary, podUID string) (*ProcessExecutor, error) {
	binary, podUID = strings.TrimSpace(binary), strings.TrimSpace(podUID)
	if binary == "" || !filepath.IsAbs(binary) || podUID == "" || len(podUID) > 80 || strings.ContainsAny(podUID, "\r\n:\x00") {
		return nil, errors.New("Guardian process executor identity is invalid")
	}
	return &ProcessExecutor{Binary: binary, PodUID: podUID}, nil
}

func (executor *ProcessExecutor) Rollout(ctx context.Context, snapshot Snapshot) (ExecutionReceipt, error) {
	return executor.execute(ctx, snapshot, "execute", snapshot.Bundle.Files)
}

func (executor *ProcessExecutor) Rollback(ctx context.Context, snapshot Snapshot) (ExecutionReceipt, error) {
	files := make(map[string][]byte, len(snapshot.CurrentMonitorData))
	for name, value := range snapshot.CurrentMonitorData {
		files[name] = []byte(value)
	}
	return executor.execute(ctx, snapshot, "restore-monitor", files)
}

func (executor *ProcessExecutor) execute(ctx context.Context, snapshot Snapshot, operation string, files map[string][]byte) (ExecutionReceipt, error) {
	if executor == nil || snapshot.Record.Validate() != nil || (operation != "execute" && operation != "restore-monitor") {
		return ExecutionReceipt{}, errors.New("Guardian executor request is invalid")
	}
	directory, err := os.MkdirTemp("", "fugue-release-guardian-")
	if err != nil {
		return ExecutionReceipt{}, err
	}
	defer os.RemoveAll(directory)
	if err := os.Chmod(directory, 0o700); err != nil {
		return ExecutionReceipt{}, err
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := bytes.TrimSpace(files[name])
		if len(value) == 0 || len(value) > 4<<20 || filepath.Base(name) != name {
			return ExecutionReceipt{}, fmt.Errorf("Guardian executor file %q is invalid", name)
		}
		if err := os.WriteFile(filepath.Join(directory, name), append(value, '\n'), 0o600); err != nil {
			return ExecutionReceipt{}, err
		}
	}
	arguments := []string{operation, directory}
	if operation == "restore-monitor" {
		arguments = append(arguments, snapshot.LKGMonitorRecordDigest)
	}
	command := exec.CommandContext(ctx, executor.Binary, arguments...)
	command.Env = append(os.Environ(),
		"FUGUE_COMPONENT_LEASE_OWNER=guardian",
		"FUGUE_RELEASE_GUARDIAN_POD_UID="+executor.PodUID,
		"FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST="+snapshot.Record.RecordDigest,
	)
	var stdout, stderr boundedBuffer
	stdout.limit, stderr.limit = 1<<20, 1<<20
	command.Stdout, command.Stderr = &stdout, &stderr
	runErr := command.Run()
	result, decodeErr := declarativerelease.DecodeExecutionResult(bytes.NewReader(bytes.TrimSpace(stdout.Bytes())))
	if decodeErr != nil {
		if runErr != nil {
			return ExecutionReceipt{}, fmt.Errorf("Guardian %s result is unknown: %w: %s", operation, runErr, strings.TrimSpace(stderr.String()))
		}
		return ExecutionReceipt{}, fmt.Errorf("decode Guardian %s receipt: %w", operation, decodeErr)
	}
	if result.Component != snapshot.Key.Component || result.ConfigSHA != snapshot.Bundle.Prepared.ConfigSHA || result.ReceiptDigest == "" {
		return ExecutionReceipt{}, errors.New("Guardian executor receipt binding is invalid")
	}
	recordDigest := snapshot.Record.RecordDigest
	if operation == "restore-monitor" && result.Status == "compensated" {
		recordDigest = snapshot.Record.LKGRecordDigest
	}
	return ExecutionReceipt{Status: result.Status, Reason: result.Reason, RecordDigest: recordDigest, ReceiptDigest: result.ReceiptDigest}, nil
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	if buffer.Buffer.Len()+len(value) > buffer.limit {
		return 0, errors.New("Guardian child output exceeded its bound")
	}
	return buffer.Buffer.Write(value)
}
