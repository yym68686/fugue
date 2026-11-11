//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// Only owned subprocesses change priority. The test runner remains unchanged.
func TestIdleRetentionProcess(t *testing.T) {
	if os.Getenv("FUGUE_TEST_IDLE_RETENTION") == "child" {
		if err := enterIdleRetention(); err != nil {
			if errors.Is(err, unix.EPERM) || errors.Is(err, unix.ENOSYS) {
				os.Exit(78)
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := verifyIdleRetention(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		// Thread creation after verification must inherit the contract too.
		runtime.LockOSThread()
		done := make(chan bool)
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			attr, err := unix.SchedGetAttr(0, 0)
			done <- err == nil && attr.Policy == unix.SCHED_IDLE && attr.Nice == 19
		}()
		if !<-done || runtime.GOMAXPROCS(0) != 1 || debug.SetMemoryLimit(-1) != 128<<20 {
			os.Exit(1)
		}
		fmt.Println("idle inheritance verified")
		os.Exit(0)
	}
	for _, forged := range []bool{false, true} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestIdleRetentionProcess$")
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, idleRetentionChild+"=") && !strings.HasPrefix(entry, "FUGUE_TEST_IDLE_RETENTION=") && !strings.HasPrefix(entry, "GOMAXPROCS=") && !strings.HasPrefix(entry, "GOMEMLIMIT=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, "FUGUE_TEST_IDLE_RETENTION=child", "GOMAXPROCS=1", "GOMEMLIMIT=128MiB")
		if forged {
			cmd.Env = append(cmd.Env, idleRetentionChild+"=1")
		}
		out, err := cmd.CombinedOutput()
		if forged {
			if err == nil || !strings.Contains(string(out), "idle thread policy unavailable") {
				t.Fatalf("forged marker accepted or unexpected refusal: %v %q", err, out)
			}
			continue
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 78 {
			t.Log("kernel/seccomp refused idle scheduling; no database access")
			continue
		}
		if err != nil || string(out) != "idle inheritance verified\n" {
			t.Fatalf("idle subprocess: %v %q", err, out)
		}
	}
}
