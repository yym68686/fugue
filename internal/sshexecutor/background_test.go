package sshexecutor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
)

func TestBackgroundServiceFixture(test *testing.T) {
	path := os.Getenv("FUGUE_TEST_BACKGROUND_SOCKET")
	if path == "" {
		return
	}
	if os.Getenv("FUGUE_TEST_BACKGROUND_CLIENT") == "true" {
		connection, err := net.DialTimeout("unix", path, time.Second)
		if err != nil {
			test.Fatal(err)
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(time.Second))
		_, _ = io.WriteString(connection, "ping")
		var reply [4]byte
		if _, err := io.ReadFull(connection, reply[:]); err != nil || string(reply[:]) != "pong" {
			test.Fatal("background service unavailable", string(reply[:]), err)
		}
		return
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		test.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(20 * time.Second))
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		_ = connection.SetDeadline(time.Now().Add(time.Second))
		var request [4]byte
		_, err = io.ReadFull(connection, request[:])
		if err == nil && string(request[:]) == "ping" {
			_, _ = io.WriteString(connection, "pong")
		}
		_ = connection.Close()
		if string(request[:]) == "stop" {
			return
		}
	}
}

func backgroundFixture(test *testing.T) (managedssh.Target, map[string]string, string) {
	test.Helper()
	target := processTarget(test)
	directory, err := os.MkdirTemp("/tmp", "fssh-")
	if err != nil {
		test.Fatal(err)
	}
	path := filepath.Join(directory, "service.sock")
	test.Cleanup(func() {
		if connection, err := net.DialTimeout("unix", path, time.Second); err == nil {
			_ = connection.SetDeadline(time.Now().Add(time.Second))
			_, _ = io.WriteString(connection, "stop")
			_ = connection.Close()
		}
		_ = os.RemoveAll(directory)
	})
	executable, err := os.Executable()
	if err != nil {
		test.Fatal(err)
	}
	command := "'" + strings.ReplaceAll(executable, "'", "'\\''") + "' -test.run=^TestBackgroundServiceFixture$"
	return target, map[string]string{"FUGUE_TEST_BACKGROUND_SOCKET": path}, command
}

func runBackgroundHelper(test *testing.T, target managedssh.Target, request managedssh.SessionRequest) (managedssh.Exit, string, error) {
	test.Helper()
	client, helper := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeHelper(ctx, helper) }()
	invocation := Invocation{Schema: "fugue.ssh-execution/v1", Target: target, Shell: "/bin/sh", Kind: request.Kind, Command: request.Command, Environment: request.Environment}
	var output bytes.Buffer
	exit, err := ExecuteHelper(ctx, client, invocation, request, managedssh.SessionIO{Stdin: strings.NewReader(""), Stdout: &output, Stderr: &output})
	select {
	case helperErr := <-done:
		if helperErr != nil {
			test.Fatal("helper did not complete", helperErr)
		}
	case <-ctx.Done():
		test.Fatal("helper did not release session", ctx.Err())
	}
	return exit, output.String(), err
}

func TestHelperBackgroundServiceSurvivesCompletedSession(test *testing.T) {
	target, environment, service := backgroundFixture(test)
	command := "nohup " + service + " </dev/null >/dev/null 2>&1 &"
	exit, output, err := runBackgroundHelper(test, target, managedssh.SessionRequest{Kind: "exec", Command: command, Environment: environment})
	if err != nil || exit.Code != 0 || output != "" {
		test.Fatal("background startup failed", exit, output, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(environment["FUGUE_TEST_BACKGROUND_SOCKET"]); err == nil {
			break
		}
		if time.Now().After(deadline) {
			test.Fatal("completed session killed background service")
		}
		time.Sleep(10 * time.Millisecond)
	}
	environment["FUGUE_TEST_BACKGROUND_CLIENT"] = "true"
	exit, output, err = runBackgroundHelper(test, target, managedssh.SessionRequest{Kind: "exec", Command: service, Environment: environment})
	if err != nil || exit.Code != 0 || !strings.Contains(output, "PASS") {
		test.Fatal("new session could not reach background Unix socket", exit, output, err)
	}
}

func TestProcessAbortedSessionKillsBackgroundGroup(test *testing.T) {
	for _, mode := range []string{"cancel", "closed-controls", "signal", "inherited-output", "failed-launch"} {
		test.Run(mode, func(test *testing.T) {
			target, environment, service := backgroundFixture(test)
			process, err := NewProcess(target, "/bin/sh")
			if err != nil {
				test.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			signals := make(chan string, 1)
			resize := make(chan managedssh.Resize)
			if mode == "closed-controls" {
				close(signals)
				close(resize)
			}
			command := "nohup " + service + " </dev/null >/dev/null 2>&1 & wait"
			if mode == "inherited-output" {
				command = "nohup " + service + " </dev/null &"
			}
			if mode == "failed-launch" {
				command = "nohup " + service + " </dev/null & sleep 0.5; exit 12"
			}
			done := make(chan struct{})
			var exit managedssh.Exit
			var runErr error
			go func() {
				exit, runErr = process.Run(ctx, target, managedssh.SessionRequest{Kind: "exec", Command: command, Environment: environment, Signals: signals, Resize: resize}, managedssh.SessionIO{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard})
				close(done)
			}()
			path := environment["FUGUE_TEST_BACKGROUND_SOCKET"]
			deadline := time.Now().Add(3 * time.Second)
			for {
				if connection, err := net.DialTimeout("unix", path, 20*time.Millisecond); err == nil {
					_ = connection.Close()
					break
				}
				if time.Now().After(deadline) {
					test.Fatal("background fixture did not start")
				}
				time.Sleep(10 * time.Millisecond)
			}
			switch mode {
			case "cancel", "closed-controls":
				cancel()
			case "signal":
				signals <- "TERM"
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				test.Fatal("aborted session was not bounded")
			}
			switch mode {
			case "cancel", "closed-controls":
				if !errors.Is(runErr, context.Canceled) {
					test.Fatal("cancellation lost", exit, runErr)
				}
			case "signal":
				if runErr != nil || exit.Signal != "TERM" {
					test.Fatal("signal lost", exit, runErr)
				}
			case "inherited-output":
				if !errors.Is(runErr, exec.ErrWaitDelay) {
					test.Fatal("inherited pipes were not bounded", exit, runErr)
				}
			case "failed-launch":
				if runErr != nil || exit.Code != 12 {
					test.Fatal("failed launch status lost", exit, runErr)
				}
			}
			deadline = time.Now().Add(time.Second)
			for {
				connection, err := net.DialTimeout("unix", path, 20*time.Millisecond)
				if err != nil {
					break
				}
				_ = connection.Close()
				if time.Now().After(deadline) {
					test.Fatal("aborted session retained background group")
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
