package main

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"fugue/internal/declarativerelease"
)

func runInstallMonitorRecord(args []string, output io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release install-monitor-record PLAN_DIR TERMINAL_RESULT")
	}
	files, err := readPlanDirectory(args[1])
	if err != nil {
		return err
	}
	terminalRaw, err := os.ReadFile(args[2])
	if err != nil {
		return err
	}
	bundle, err := decodeMonitorBundle(files, terminalRaw)
	if err != nil {
		return err
	}
	release, err := selectedRelease(bundle.Plan, bundle.Record.Component)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lease, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	held, err := lease.acquire(ctx, release, bundle.Record.ConfigSHA)
	if err != nil {
		return err
	}
	store, err := newMonitorStore()
	if err == nil {
		_, err = store.persistVerified(ctx, release, files, bundle.Terminal)
	}
	releaseErr := lease.release(ctx, held)
	if err != nil {
		return err
	}
	if releaseErr != nil {
		return releaseErr
	}
	raw, err := declarativerelease.CanonicalJSON(bundle.Record)
	if err != nil {
		return err
	}
	_, err = output.Write(append(raw, '\n'))
	return err
}
