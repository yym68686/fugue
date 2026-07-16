//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExecCommandRunnerConcurrentShortCommands(t *testing.T) {
	const commands = 32
	directory := t.TempDir()
	errorsByCommand := make(chan error, commands)
	for index := 0; index < commands; index++ {
		go func() {
			output, err := (execCommandRunner{}).Run(
				context.Background(),
				directory,
				nil,
				nil,
				16,
				"sh",
				"-c",
				"printf ok",
			)
			if err == nil && string(output) != "ok" {
				err = errors.New("short command output drifted")
			}
			errorsByCommand <- err
		}()
	}
	for index := 0; index < commands; index++ {
		if err := <-errorsByCommand; err != nil {
			t.Fatalf("short command failed: %v", err)
		}
	}
}

func TestExecCommandRunnerKillsOrphanedPipeDescendant(t *testing.T) {
	pidFile := t.TempDir() + "/descendant.pid"
	started := time.Now()
	_, err := (execCommandRunner{}).Run(
		context.Background(),
		t.TempDir(),
		nil,
		nil,
		64,
		"sh",
		"-c",
		`sleep 30 & printf '%s\n' "$!" > "$1"; exit 0`,
		"sh",
		pidFile,
	)
	if !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("orphaned pipe error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > evidenceCommandWaitDelay+2*time.Second {
		t.Fatalf("orphaned pipe cleanup took %s", elapsed)
	}
	contents, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(contents)))
	if parseErr != nil || pid <= 0 {
		t.Fatalf("descendant PID = %q: %v", contents, parseErr)
	}
	defer syscall.Kill(pid, syscall.SIGKILL)
	deadline := time.Now().Add(2 * time.Second)
	for {
		killErr := syscall.Kill(pid, 0)
		if errors.Is(killErr, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant %d survived process-group cleanup: %v", pid, killErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
