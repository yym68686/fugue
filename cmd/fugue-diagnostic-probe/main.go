package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"fugue/internal/diagnosticprobe"
	"fugue/internal/livediagnostics"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	raw := []byte(os.Getenv(livediagnostics.ProbeRequestEnv))
	if len(raw) > 64<<10 {
		fmt.Fprintln(os.Stderr, "probe request exceeds 64 KiB")
		os.Exit(1)
	}
	var request livediagnostics.ProbeRequest
	if err := livediagnostics.DecodeStrict(raw, &request); err != nil {
		fmt.Fprintln(os.Stderr, "invalid probe request:", err)
		os.Exit(1)
	}
	report, err := diagnosticprobe.Collect(ctx, request)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe failed:", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		os.Exit(1)
	}
}
