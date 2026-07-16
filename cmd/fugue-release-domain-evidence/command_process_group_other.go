//go:build !darwin && !linux

package main

import (
	"fmt"
	"os/exec"
	"runtime"
	"time"
)

const evidenceCommandWaitDelay = 2 * time.Second

type evidenceCommandGroup struct{}

func startEvidenceCommandGroup() (*evidenceCommandGroup, error) {
	return nil, fmt.Errorf("process-group fencing is unsupported on %s", runtime.GOOS)
}

func configureEvidenceCommand(_ *exec.Cmd, _ *evidenceCommandGroup) {}

func cleanupEvidenceCommandGroup(_ *evidenceCommandGroup) error { return nil }
