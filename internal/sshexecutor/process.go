package sshexecutor

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"fugue/internal/managedssh"

	"github.com/creack/pty"
	"github.com/pkg/sftp"
)

type Process struct {
	target managedssh.Target
	shell  string
	uid    int
}

func NewProcess(target managedssh.Target, shell string) (*Process, error) {
	if !filepath.IsAbs(shell) || !filepath.IsAbs(target.WorkingDirectory) {
		return nil, managedssh.ErrConfiguration
	}
	account, err := user.Lookup(target.User)
	if err != nil {
		account, err = user.LookupId(target.User)
	}
	if err != nil {
		return nil, managedssh.ErrCapabilities
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil || uid != os.Geteuid() {
		return nil, managedssh.ErrUnauthorized
	}
	process := &Process{target: target, shell: shell, uid: uid}
	if _, err := process.Validate(context.Background(), target); err != nil {
		return nil, err
	}
	return process, nil
}

func (process *Process) Validate(ctx context.Context, target managedssh.Target) (managedssh.Capabilities, error) {
	if err := ctx.Err(); err != nil {
		return managedssh.Capabilities{}, err
	}
	if target != process.target || os.Geteuid() != process.uid {
		return managedssh.Capabilities{}, managedssh.ErrUnauthorized
	}
	shell, err := os.Stat(process.shell)
	if err != nil || !shell.Mode().IsRegular() || shell.Mode().Perm()&0o111 == 0 {
		return managedssh.Capabilities{}, managedssh.ErrCapabilities
	}
	directory, err := os.Stat(target.WorkingDirectory)
	if err != nil || !directory.IsDir() {
		return managedssh.Capabilities{}, managedssh.ErrCapabilities
	}
	return managedssh.Capabilities{Exec: true, Shell: true, PTY: true, SFTP: true, Signals: true, Forward: true}, nil
}

func (process *Process) Run(ctx context.Context, target managedssh.Target, request managedssh.SessionRequest, streams managedssh.SessionIO) (managedssh.Exit, error) {
	if _, err := process.Validate(ctx, target); err != nil {
		return managedssh.Exit{}, err
	}
	if request.Kind == "sftp" {
		return runSFTP(ctx, target.WorkingDirectory, streams)
	}
	var command *exec.Cmd
	switch request.Kind {
	case "shell":
		command = exec.Command(process.shell, "-l")
	case "exec":
		command = exec.Command(process.shell, "-c", request.Command)
	default:
		return managedssh.Exit{}, managedssh.ErrCapabilities
	}
	command.Dir = target.WorkingDirectory
	command.Env = executionEnvironment(request)
	var terminal *os.File
	var terminalOutput chan struct{}
	var input io.WriteCloser
	if request.Terminal != nil {
		master, slave, err := pty.Open()
		if err != nil {
			return managedssh.Exit{}, err
		}
		terminal = master
		defer master.Close()
		defer slave.Close()
		if err := pty.Setsize(master, &pty.Winsize{Cols: uint16(request.Terminal.Columns), Rows: uint16(request.Terminal.Rows)}); err != nil {
			return managedssh.Exit{}, err
		}
		if err := setTerminalModes(slave, request.Terminal.Modes); err != nil {
			return managedssh.Exit{}, err
		}
		command.Stdin = slave
		command.Stdout = slave
		command.Stderr = slave
		command.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
		if err := command.Start(); err != nil {
			return managedssh.Exit{}, err
		}
		_ = slave.Close()
		terminalOutput = make(chan struct{})
		go func() { defer close(terminalOutput); _, _ = io.Copy(streams.Stdout, master) }()
		go func() { _, _ = io.Copy(master, streams.Stdin) }()
	} else {
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		command.Stdout = streams.Stdout
		command.Stderr = streams.Stderr
		command.WaitDelay = time.Second
		var err error
		input, err = command.StdinPipe()
		if err != nil {
			return managedssh.Exit{}, err
		}
		defer input.Close()
		if err := command.Start(); err != nil {
			return managedssh.Exit{}, err
		}
		go func() { _, _ = io.Copy(input, streams.Stdin); _ = input.Close() }()
	}
	processDone := make(chan struct{})
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		for {
			select {
			case <-processDone:
				return
			case <-ctx.Done():
				_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
				return
			case signal, open := <-request.Signals:
				if !open {
					return
				}
				if value, ok := processSignal(signal); ok {
					_ = syscall.Kill(-command.Process.Pid, value)
				}
			case resize, open := <-request.Resize:
				if !open {
					return
				}
				if terminal != nil && resize.Columns > 0 && resize.Columns <= 65535 && resize.Rows > 0 && resize.Rows <= 65535 {
					_ = pty.Setsize(terminal, &pty.Winsize{Cols: uint16(resize.Columns), Rows: uint16(resize.Rows)})
				}
			}
		}
	}()
	err := command.Wait()
	close(processDone)
	<-controlDone
	_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	if terminal != nil {
		select {
		case <-terminalOutput:
		case <-time.After(time.Second):
			_ = terminal.Close()
			<-terminalOutput
		}
	}
	if ctx.Err() != nil {
		return managedssh.Exit{}, ctx.Err()
	}
	if err == nil {
		return managedssh.Exit{}, nil
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return managedssh.Exit{}, err
	}
	status, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok {
		return managedssh.Exit{Code: 255}, nil
	}
	if status.Signaled() {
		return managedssh.Exit{Code: uint32(128 + int(status.Signal())), Signal: signalName(status.Signal())}, nil
	}
	return managedssh.Exit{Code: uint32(status.ExitStatus())}, nil
}

func (process *Process) Forward(ctx context.Context, target managedssh.Target, host string, port uint32) (net.Conn, error) {
	if _, err := process.Validate(ctx, target); err != nil {
		return nil, err
	}
	address := net.ParseIP(host)
	if address == nil || !address.IsLoopback() || port == 0 || port > 65535 {
		return nil, managedssh.ErrUnauthorized
	}
	return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", net.JoinHostPort(address.String(), strconv.Itoa(int(port))))
}

func executionEnvironment(request managedssh.SessionRequest) []string {
	environment := map[string]string{}
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			environment[name] = value
		}
	}
	for name, value := range request.Environment {
		environment[name] = value
	}
	if request.Terminal != nil {
		environment["TERM"] = request.Terminal.Name
	}
	names := make([]string, 0, len(environment))
	for name := range environment {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, name+"="+environment[name])
	}
	return result
}

type transferStream struct {
	io.Reader
	io.Writer
	close func() error
}

func (stream transferStream) Close() error { return stream.close() }

func runSFTP(ctx context.Context, directory string, streams managedssh.SessionIO) (managedssh.Exit, error) {
	reader, writer := io.Pipe()
	go func() { _, err := io.Copy(writer, streams.Stdin); _ = writer.CloseWithError(err) }()
	var once sync.Once
	closeStream := func() error { once.Do(func() { _ = reader.Close(); _ = writer.Close() }); return nil }
	defer closeStream()
	server, err := sftp.NewServer(transferStream{Reader: reader, Writer: streams.Stdout, close: closeStream}, sftp.WithServerWorkingDirectory(directory))
	if err != nil {
		return managedssh.Exit{}, err
	}
	defer server.Close()
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	err = server.Serve()
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return managedssh.Exit{}, err
}

func processSignal(name string) (syscall.Signal, bool) {
	signals := map[string]syscall.Signal{"ABRT": syscall.SIGABRT, "ALRM": syscall.SIGALRM, "FPE": syscall.SIGFPE, "HUP": syscall.SIGHUP, "ILL": syscall.SIGILL, "INT": syscall.SIGINT, "KILL": syscall.SIGKILL, "PIPE": syscall.SIGPIPE, "QUIT": syscall.SIGQUIT, "SEGV": syscall.SIGSEGV, "TERM": syscall.SIGTERM, "USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2}
	signal, found := signals[name]
	return signal, found
}

func signalName(value syscall.Signal) string {
	for _, name := range []string{"ABRT", "ALRM", "FPE", "HUP", "ILL", "INT", "KILL", "PIPE", "QUIT", "SEGV", "TERM", "USR1", "USR2"} {
		if signal, _ := processSignal(name); signal == value {
			return name
		}
	}
	return "TERM"
}
