package sshexecutor

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/managedssh"
)

func TestHelperProbeExecutesRuntimeInsteadOfTrustingExecutableFileMetadata(test *testing.T) {
	for _, broken := range []bool{false, true} {
		test.Run(map[bool]string{false: "supported", true: "executable-but-unusable-shell"}[broken], func(test *testing.T) {
			target := processTarget(test)
			shell := "/bin/sh"
			if broken {
				shell = filepath.Join(test.TempDir(), "shell")
				if err := os.WriteFile(shell, []byte("#!/bin/sh\nexit 47\n"), 0o700); err != nil {
					test.Fatal(err)
				}
				process, err := NewProcess(target, shell)
				if err != nil {
					test.Fatal("fixture must pass metadata-only validation", err)
				}
				metadata, err := process.Validate(context.Background(), target)
				if err != nil || !metadata.Exec || !metadata.Shell {
					test.Fatal("fixture did not reproduce the incomplete validation", err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, helper := net.Pipe()
			done := make(chan error, 1)
			go func() { done <- ServeHelper(ctx, helper) }()
			capabilities, err := ProbeHelper(ctx, client, target, shell)
			if broken {
				if !errors.Is(err, managedssh.ErrCapabilities) || capabilities.Exec {
					test.Fatal("unusable runtime advertised execution support", capabilities, err)
				}
			} else if err != nil || !capabilities.Exec || !capabilities.Shell || !capabilities.PTY || !capabilities.SFTP || !capabilities.Signals || !capabilities.Forward {
				test.Fatal("real helper failed bounded runtime probe", capabilities, err)
			}
			if helperErr := <-done; (helperErr != nil) != broken {
				test.Fatal("helper probe completion differs", helperErr)
			}
		})
	}
}

func TestHelperProbeBoundsMissingOrUnresponsiveRuntime(test *testing.T) {
	for _, closed := range []bool{false, true} {
		client, helper := net.Pipe()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		if closed {
			helper.Close()
		}
		started := time.Now()
		if capabilities, err := ProbeHelper(ctx, client, processTarget(test), "/bin/sh"); err == nil || capabilities.Exec || time.Since(started) > time.Second {
			test.Fatal("missing or unresponsive helper produced positive evidence", capabilities, err)
		}
		cancel()
		helper.Close()
	}
}
