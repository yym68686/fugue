package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"fugue/internal/sshexecutor"
)

type standardStream struct{}

func (standardStream) Read(value []byte) (int, error)  { return os.Stdin.Read(value) }
func (standardStream) Write(value []byte) (int, error) { return os.Stdout.Write(value) }
func (standardStream) Close() error                    { _ = os.Stdin.Close(); return os.Stdout.Close() }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err := sshexecutor.ServeHelper(ctx, standardStream{}); err != nil {
		fmt.Fprintln(os.Stderr, "managed SSH helper stopped")
		os.Exit(1)
	}
}
