package sshexecutor

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
)

func TestExecutionProtocolBackpressureAndExit(test *testing.T) {
	target := processTarget(test)
	client, helper := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeHelper(ctx, helper) }()
	data := strings.Repeat("payload-", 32768)
	request := managedssh.SessionRequest{Kind: "exec", Command: "cat; printf helper-stderr >&2; exit 9"}
	invocation := Invocation{Schema: "fugue.ssh-execution/v1", Target: target, Shell: "/bin/sh", Kind: request.Kind, Command: request.Command}
	var output, diagnostic bytes.Buffer
	exit, err := ExecuteHelper(ctx, client, invocation, request, managedssh.SessionIO{Stdin: strings.NewReader(data), Stdout: &output, Stderr: &diagnostic})
	if err != nil || exit.Code != 9 || output.String() != data || diagnostic.String() != "helper-stderr" {
		test.Fatal("helper protocol lost data or exit status", exit, len(output.Bytes()), diagnostic.String(), err)
	}
	if err := <-done; err != nil {
		test.Fatal(err)
	}
}

func TestExecutionProtocolSignalDoesNotWaitForStdinConsumption(test *testing.T) {
	target := processTarget(test)
	client, helper := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeHelper(ctx, helper) }()
	signals := make(chan string, 1)
	go func() { time.Sleep(100 * time.Millisecond); signals <- "TERM" }()
	request := managedssh.SessionRequest{Kind: "exec", Command: "sleep 60", Signals: signals}
	invocation := Invocation{Schema: "fugue.ssh-execution/v1", Target: target, Shell: "/bin/sh", Kind: request.Kind, Command: request.Command}
	exit, err := ExecuteHelper(ctx, client, invocation, request, managedssh.SessionIO{Stdin: strings.NewReader(strings.Repeat("input", 32768)), Stdout: io.Discard, Stderr: io.Discard})
	if err != nil || exit.Signal != "TERM" {
		test.Fatal("blocked stdin prevented control delivery", exit, err)
	}
	if err := <-done; err != nil {
		test.Fatal(err)
	}
}

func TestExecutionProtocolRejectsOversizedFrames(test *testing.T) {
	var encoded bytes.Buffer
	encoded.WriteByte(frameStart)
	if err := binary.Write(&encoded, binary.BigEndian, uint32(maxFrameSize+1)); err != nil {
		test.Fatal(err)
	}
	if _, _, err := readFrame(&encoded); err == nil {
		test.Fatal("unbounded frame accepted")
	}
}
