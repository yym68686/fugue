package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"fugue/internal/diagnosticprobe"
	"fugue/internal/livediagnostics"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if len(os.Args) == 3 && os.Args[1] == "--loopback-metrics" {
		port, err := strconv.Atoi(os.Args[2])
		if err != nil {
			os.Exit(1)
		}
		raw, err := diagnosticprobe.LoopbackMetrics(ctx, port)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if _, err := os.Stdout.Write(raw); err != nil {
			os.Exit(1)
		}
		return
	}
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
	if err := writeReport(os.Stdout, report); err != nil {
		os.Exit(1)
	}
}

// JSON reports are also read by line-oriented log collectors. Insert sparse
// whitespace at JSON token boundaries so arrays do not become multi-MiB lines.
// String contents and numeric precision remain byte-for-byte unchanged.
func writeReport(w io.Writer, report livediagnostics.ProbeReport) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	var output bytes.Buffer
	output.Grow(len(raw) + len(raw)/(32<<10) + 1)
	quoted, escaped, column := false, false, 0
	for _, ch := range raw {
		output.WriteByte(ch)
		column++
		if quoted {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				quoted = false
			}
			continue
		}
		if ch == '"' {
			quoted = true
			continue
		}
		if column >= 32<<10 && (ch == ',' || ch == '}' || ch == ']' || ch == '{' || ch == '[' || ch == ':') {
			output.WriteByte('\n')
			column = 0
		}
	}
	output.WriteByte('\n')
	_, err = w.Write(output.Bytes())
	return err
}
