//go:build !linux && !darwin

package diagnosticprobe

import "os/exec"

func configureCommandCancellation(cmd *exec.Cmd) {}
