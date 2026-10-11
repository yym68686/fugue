package sshexecutor

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"fugue/internal/managedssh"
	"github.com/creack/pty"
)

const frameCapabilities byte = 12

func (process *Process) ProbeCapabilities(ctx context.Context) (managedssh.Capabilities, error) {
	capabilities, err := process.Validate(ctx, process.target)
	if err != nil {
		return managedssh.Capabilities{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, process.shell, "-c", ":")
	command.Dir = process.target.WorkingDirectory
	command.Env = process.executionEnvironment(managedssh.SessionRequest{})
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = 100 * time.Millisecond
	command.Cancel = func() error {
		if command.Process != nil {
			return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	err = command.Run()
	if command.Process != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	if err != nil {
		return managedssh.Capabilities{}, managedssh.ErrCapabilities
	}
	master, slave, err := pty.Open()
	capabilities.PTY = err == nil
	if err == nil {
		capabilities.PTY = pty.Setsize(master, &pty.Winsize{Cols: 80, Rows: 24}) == nil
		_ = slave.Close()
		_ = master.Close()
	}
	socket, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	capabilities.Forward = err == nil
	if err == nil {
		_ = syscall.Close(socket)
	}
	capabilities.Signals = syscall.Kill(os.Getpid(), 0) == nil
	return capabilities, nil
}

func ProbeHelper(ctx context.Context, stream io.ReadWriteCloser, target managedssh.Target, shell string) (managedssh.Capabilities, error) {
	defer stream.Close()
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	defer stop()
	payload, err := json.Marshal(Invocation{Schema: "fugue.ssh-execution/v1", Target: target, Shell: shell, Kind: "probe"})
	if err != nil {
		return managedssh.Capabilities{}, err
	}
	writer := &frameWriter{writer: stream}
	if err := writer.send(frameStart, payload); err != nil {
		return managedssh.Capabilities{}, err
	}
	kind, payload, err := readFrame(stream)
	if err != nil {
		return managedssh.Capabilities{}, err
	}
	var capabilities managedssh.Capabilities
	if kind != frameCapabilities || json.Unmarshal(payload, &capabilities) != nil {
		return managedssh.Capabilities{}, managedssh.ErrCapabilities
	}
	return capabilities, nil
}
