//go:build darwin || linux

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

const (
	evidenceCommandWaitDelay = 2 * time.Second
	evidenceAnchorArgument   = "--fugue-release-domain-evidence-command-group-anchor"
	evidenceAnchorEnv        = "FUGUE_RELEASE_DOMAIN_EVIDENCE_COMMAND_GROUP_ANCHOR"
	evidenceAnchorToken      = "private-v1"
)

type evidenceCommandGroup struct {
	anchor *exec.Cmd
	gate   *os.File
	id     int
}

func init() {
	if os.Getenv(evidenceAnchorEnv) != evidenceAnchorToken || len(os.Args) != 2 || os.Args[1] != evidenceAnchorArgument {
		return
	}
	var gate [1]byte
	_, _ = os.Stdin.Read(gate[:])
	os.Exit(0)
}

func startEvidenceCommandGroup() (*evidenceCommandGroup, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	readGate, writeGate, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	anchor := exec.Command(executable, evidenceAnchorArgument)
	anchor.Env = []string{evidenceAnchorEnv + "=" + evidenceAnchorToken}
	anchor.Stdin = readGate
	anchor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := anchor.Start(); err != nil {
		_ = readGate.Close()
		_ = writeGate.Close()
		return nil, err
	}
	if err := readGate.Close(); err != nil {
		_ = syscall.Kill(-anchor.Process.Pid, syscall.SIGKILL)
		_ = writeGate.Close()
		_ = anchor.Wait()
		return nil, err
	}
	return &evidenceCommandGroup{anchor: anchor, gate: writeGate, id: anchor.Process.Pid}, nil
}

func configureEvidenceCommand(command *exec.Cmd, group *evidenceCommandGroup) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: group.id}
	command.WaitDelay = evidenceCommandWaitDelay
	command.Cancel = func() error {
		return killEvidenceCommandProcessGroup(group.id)
	}
}

func cleanupEvidenceCommandGroup(group *evidenceCommandGroup) error {
	killErr := killEvidenceCommandProcessGroup(group.id)
	closeErr := group.gate.Close()
	_ = group.anchor.Wait()
	if killErr != nil {
		return killErr
	}
	if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
		return closeErr
	}
	return nil
}

func killEvidenceCommandProcessGroup(groupID int) error {
	err := syscall.Kill(-groupID, syscall.SIGKILL)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
