package sshexecutor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/sshidentity"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type discardAudit struct{}

func (discardAudit) Record(context.Context, managedssh.AuditEvent) error { return nil }

func processTarget(test *testing.T) managedssh.Target {
	test.Helper()
	account, err := user.Current()
	if err != nil {
		test.Fatal(err)
	}
	return managedssh.Target{TenantID: "tenant_example", AppID: "app_example", Namespace: "test", PodName: "workspace", PodUID: "pod-uid", Container: "app", ContainerID: "container-id", User: account.Username, WorkingDirectory: test.TempDir()}
}

func TestProcessExecutionDoesNotWaitForClientInputEOF(test *testing.T) {
	target := processTarget(test)
	executor, err := NewProcess(target, "/bin/sh")
	if err != nil {
		test.Fatal(err)
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	exit, err := executor.Run(ctx, target, managedssh.SessionRequest{Kind: "exec", Command: "printf ok; printf warning >&2; exit 12"}, managedssh.SessionIO{Stdin: reader, Stdout: &stdout, Stderr: &stderr})
	if err != nil || exit.Code != 12 || stdout.String() != "ok" || stderr.String() != "warning" {
		test.Fatal("process completion or stream handling failed", exit, stdout.String(), stderr.String(), err)
	}
}

func TestProcessPTYAndSignals(test *testing.T) {
	target := processTarget(test)
	executor, err := NewProcess(target, "/bin/sh")
	if err != nil {
		test.Fatal(err)
	}
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := managedssh.SessionRequest{Kind: "exec", Command: "stty size; stty -a", Terminal: &managedssh.Terminal{Name: "xterm", Columns: 99, Rows: 37, Modes: map[uint8]uint32{ssh.ECHO: 0}}}
	exit, err := executor.Run(ctx, target, request, managedssh.SessionIO{Stdin: strings.NewReader(""), Stdout: &output, Stderr: io.Discard})
	if err != nil || exit.Code != 0 || !strings.Contains(output.String(), "37 99") || !strings.Contains(output.String(), "-echo") {
		test.Fatal("real PTY was not configured", output.String(), exit, err)
	}
	signals := make(chan string, 1)
	signals <- "TERM"
	exit, err = executor.Run(ctx, target, managedssh.SessionRequest{Kind: "exec", Command: "sleep 60", Signals: signals}, managedssh.SessionIO{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard})
	if err != nil || exit.Signal != "TERM" {
		test.Fatal("real process signal failed", exit, err)
	}
}

func TestProcessTargetAndUserBinding(test *testing.T) {
	target := processTarget(test)
	executor, err := NewProcess(target, "/bin/sh")
	if err != nil {
		test.Fatal(err)
	}
	other := target
	other.ContainerID = "another-container"
	if _, err := executor.Validate(context.Background(), other); !errors.Is(err, managedssh.ErrUnauthorized) {
		test.Fatal("executor crossed target binding", err)
	}
	other = target
	other.User = strconv.Itoa(os.Geteuid() + 10000)
	if _, err := NewProcess(other, "/bin/sh"); err == nil {
		test.Fatal("executor silently changed operating-system user")
	}
}

func TestManagedSSHWithRealProcessAndSFTP(test *testing.T) {
	target := processTarget(test)
	executor, err := NewProcess(target, "/bin/sh")
	if err != nil {
		test.Fatal(err)
	}
	keys, err := sshidentity.NewKeyring("test", map[string][]byte{"test": bytes.Repeat([]byte{2}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	host, err := keys.Create(target.TenantID, target.AppID, time.Now())
	if err != nil {
		test.Fatal(err)
	}
	hostSigner, err := keys.Signer(host)
	if err != nil {
		test.Fatal(err)
	}
	userKey, err := keys.Create(target.TenantID, "user", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	userSigner, err := keys.Signer(userKey)
	if err != nil {
		test.Fatal(err)
	}
	configuration := managedssh.Configuration{Schema: "fugue.managed-ssh/v1", Generation: 1, Identity: host.Identity, Target: target, Keys: []managedssh.KeyGrant{{ID: "key", PublicKey: userKey.Identity.PublicKey, ExpiresAt: time.Now().Add(time.Minute)}}, ValidUntil: time.Now().Add(time.Hour), AllowPTY: true, AllowSFTP: true, MaxConnections: 4, MaxConnectionsPerIP: 2, MaxChannels: 4}
	server, err := managedssh.NewServer(executor, discardAudit{})
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), configuration, hostSigner); err != nil {
		test.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	defer func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil {
				test.Error(err)
			}
		case <-time.After(3 * time.Second):
			test.Error("server shutdown timed out")
		}
	}()
	client, err := ssh.Dial("tcp", listener.Addr().String(), &ssh.ClientConfig{User: target.User, Auth: []ssh.AuthMethod{ssh.PublicKeys(userSigner)}, HostKeyCallback: ssh.FixedHostKey(hostSigner.PublicKey()), Timeout: time.Second})
	if err != nil {
		test.Fatal(err)
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	output, err := session.Output("printf platform-managed")
	_ = session.Close()
	if err != nil || string(output) != "platform-managed" {
		test.Fatal("real SSH execution failed", string(output), err)
	}
	transfer, err := sftp.NewClient(client)
	if err != nil {
		test.Fatal(err)
	}
	defer transfer.Close()
	remote, err := transfer.Create("uploaded.txt")
	if err != nil {
		test.Fatal(err)
	}
	if _, err := remote.Write([]byte("file-transfer-payload")); err != nil {
		test.Fatal(err)
	}
	if err := remote.Close(); err != nil {
		test.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(target.WorkingDirectory, "uploaded.txt"))
	if err != nil || string(content) != "file-transfer-payload" {
		test.Fatal("SFTP wrote wrong filesystem", string(content), err)
	}
	if err := transfer.Rename("uploaded.txt", "renamed.txt"); err != nil {
		test.Fatal(err)
	}
	download, err := transfer.Open("renamed.txt")
	if err != nil {
		test.Fatal(err)
	}
	content, err = io.ReadAll(download)
	_ = download.Close()
	if err != nil || string(content) != "file-transfer-payload" {
		test.Fatal("SFTP download failed", err)
	}
	if err := transfer.Remove("renamed.txt"); err != nil {
		test.Fatal(err)
	}
}
