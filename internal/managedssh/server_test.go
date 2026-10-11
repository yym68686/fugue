package managedssh

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/sshidentity"

	"golang.org/x/crypto/ssh"
)

type testExecutor struct {
	run           func(context.Context, Target, SessionRequest, SessionIO) (Exit, error)
	validateError error
	forward       func(context.Context, Target, string, uint32) (net.Conn, error)
}

func (executor *testExecutor) Validate(ctx context.Context, target Target) (Capabilities, error) {
	return Capabilities{Exec: true, Shell: true, PTY: true, SFTP: true, Signals: true, Forward: true}, executor.validateError
}
func (executor *testExecutor) Run(ctx context.Context, target Target, request SessionRequest, streams SessionIO) (Exit, error) {
	if executor.run != nil {
		return executor.run(ctx, target, request, streams)
	}
	return Exit{}, nil
}
func (executor *testExecutor) Forward(ctx context.Context, target Target, host string, port uint32) (net.Conn, error) {
	if executor.forward == nil {
		return nil, errors.New("not configured")
	}
	return executor.forward(ctx, target, host, port)
}

type testAudit struct {
	mu      sync.Mutex
	events  []AuditEvent
	failure bool
}

func (audit *testAudit) Record(ctx context.Context, event AuditEvent) error {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	if audit.failure {
		return errors.New("audit unavailable")
	}
	audit.events = append(audit.events, event)
	return nil
}

func testConfiguration(test *testing.T) (Configuration, ssh.Signer, ssh.Signer) {
	test.Helper()
	keyring, err := sshidentity.NewKeyring("storage", map[string][]byte{"storage": bytes.Repeat([]byte{33}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	identity, err := keyring.Create("tenant_sample", "app_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	hostSigner, err := keyring.Signer(identity)
	if err != nil {
		test.Fatal(err)
	}
	userIdentity, err := keyring.Create("tenant_sample", "user_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	userSigner, err := keyring.Signer(userIdentity)
	if err != nil {
		test.Fatal(err)
	}
	configuration := Configuration{
		Schema: "fugue.managed-ssh/v1", Generation: 1, Identity: identity.Identity,
		Target:     Target{TenantID: "tenant_sample", AppID: "app_sample", Namespace: "workloads", PodName: "workspace", PodUID: "pod-uid", Container: "workspace", ContainerID: "containerd://container-id", User: "app", WorkingDirectory: "/workspace"},
		Keys:       []KeyGrant{{ID: "grant", PublicKey: string(ssh.MarshalAuthorizedKey(userSigner.PublicKey())), ExpiresAt: time.Now().Add(time.Hour)}},
		ValidUntil: time.Now().Add(2 * time.Hour), AllowPTY: true, AllowSFTP: true,
		ForwardRules: []ForwardRule{{Host: "127.0.0.1", Port: 9000}}, AllowedEnvironment: []string{"LANG"},
		MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4,
	}
	return configuration, hostSigner, userSigner
}

func serveForTest(test *testing.T, executor Executor, audit AuditSink, configuration Configuration, signer ssh.Signer) (*Server, string) {
	test.Helper()
	server, err := NewServer(executor, audit)
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), configuration, signer); err != nil {
		test.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	test.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil {
				test.Error(err)
			}
		case <-time.After(3 * time.Second):
			test.Error("managed SSH did not shut down")
		}
	})
	return server, listener.Addr().String()
}

func connectForTest(test *testing.T, address, user string, host, userKey ssh.Signer) *ssh.Client {
	test.Helper()
	client, err := ssh.Dial("tcp", address, &ssh.ClientConfig{User: user, Auth: []ssh.AuthMethod{ssh.PublicKeys(userKey)}, HostKeyCallback: ssh.FixedHostKey(host.PublicKey()), Timeout: 2 * time.Second})
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = client.Close() })
	return client
}

func TestManagedSSHExecAndExitStatus(test *testing.T) {
	configuration, hostSigner, userSigner := testConfiguration(test)
	seen := make(chan SessionRequest, 1)
	executor := &testExecutor{run: func(ctx context.Context, target Target, request SessionRequest, streams SessionIO) (Exit, error) {
		seen <- request
		input, err := io.ReadAll(streams.Stdin)
		if err != nil {
			return Exit{}, err
		}
		_, _ = streams.Stdout.Write(append([]byte("output:"), input...))
		_, _ = io.WriteString(streams.Stderr, "diagnostic")
		return Exit{Code: 7}, nil
	}}
	audit := &testAudit{}
	_, address := serveForTest(test, executor, audit, configuration, hostSigner)
	client := connectForTest(test, address, "app", hostSigner, userSigner)
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer session.Close()
	session.Stdin = strings.NewReader("payload")
	var output, diagnostic bytes.Buffer
	session.Stdout = &output
	session.Stderr = &diagnostic
	if err := session.Setenv("LANG", "C.UTF-8"); err != nil {
		test.Fatal(err)
	}
	if err := session.Setenv("LD_PRELOAD", "/bad"); err == nil {
		test.Fatal("unapproved environment accepted")
	}
	err = session.Run("printf user-command")
	var exitError *ssh.ExitError
	if !errors.As(err, &exitError) || exitError.ExitStatus() != 7 || output.String() != "output:payload" || diagnostic.String() != "diagnostic" {
		test.Fatal("incorrect execution stream or status", output.String(), diagnostic.String(), err)
	}
	request := <-seen
	if request.Kind != "exec" || request.Command != "printf user-command" || request.Environment["LANG"] != "C.UTF-8" {
		test.Fatal(request)
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	for _, event := range audit.events {
		if strings.Contains(event.Reason, "user-command") || strings.Contains(event.Reason, "payload") {
			test.Fatal("audit exposed command or output")
		}
	}
}

func TestManagedSSHPTYResizeAndSignals(test *testing.T) {
	configuration, hostSigner, userSigner := testConfiguration(test)
	executor := &testExecutor{run: func(ctx context.Context, target Target, request SessionRequest, streams SessionIO) (Exit, error) {
		if request.Terminal == nil || request.Terminal.Name != "xterm" || request.Terminal.Columns != 80 || request.Terminal.Rows != 24 || request.Terminal.Modes[ssh.ECHO] != 0 {
			return Exit{}, errors.New("terminal metadata missing")
		}
		select {
		case resized := <-request.Resize:
			if resized.Columns != 120 || resized.Rows != 40 {
				return Exit{}, errors.New("wrong size")
			}
		case <-ctx.Done():
			return Exit{}, ctx.Err()
		}
		select {
		case signal := <-request.Signals:
			if signal != "TERM" {
				return Exit{}, errors.New("wrong signal")
			}
		case <-ctx.Done():
			return Exit{}, ctx.Err()
		}
		return Exit{Code: 143, Signal: "TERM"}, nil
	}}
	_, address := serveForTest(test, executor, &testAudit{}, configuration, hostSigner)
	client := connectForTest(test, address, "app", hostSigner, userSigner)
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer session.Close()
	if err := session.RequestPty("xterm", 24, 80, ssh.TerminalModes{ssh.ECHO: 0}); err != nil {
		test.Fatal(err)
	}
	if err := session.Shell(); err != nil {
		test.Fatal(err)
	}
	if err := session.WindowChange(40, 120); err != nil {
		test.Fatal(err)
	}
	if err := session.Signal(ssh.SIGTERM); err != nil {
		test.Fatal(err)
	}
	err = session.Wait()
	var exitError *ssh.ExitError
	if !errors.As(err, &exitError) || exitError.Signal() != "TERM" {
		test.Fatal("signal/terminal handling failed", err)
	}
}

func TestManagedSSHRevocationClosesActiveExecution(test *testing.T) {
	configuration, hostSigner, userSigner := testConfiguration(test)
	started := make(chan struct{})
	stopped := make(chan struct{})
	executor := &testExecutor{run: func(ctx context.Context, target Target, request SessionRequest, streams SessionIO) (Exit, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return Exit{}, ctx.Err()
	}}
	server, address := serveForTest(test, executor, &testAudit{}, configuration, hostSigner)
	client := connectForTest(test, address, "app", hostSigner, userSigner)
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer session.Close()
	if err := session.Start("long-running"); err != nil {
		test.Fatal(err)
	}
	<-started
	configuration.Generation++
	configuration.Keys = nil
	if err := server.Activate(context.Background(), configuration, hostSigner); err != nil {
		test.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		test.Fatal("revocation did not cancel execution")
	}
	if _, err := ssh.Dial("tcp", address, &ssh.ClientConfig{User: "app", Auth: []ssh.AuthMethod{ssh.PublicKeys(userSigner)}, HostKeyCallback: ssh.FixedHostKey(hostSigner.PublicKey()), Timeout: time.Second}); err == nil {
		test.Fatal("revoked key authenticated")
	}
}

func TestManagedSSHInvalidUpdatePreservesServingState(test *testing.T) {
	configuration, hostSigner, userSigner := testConfiguration(test)
	executor := &testExecutor{}
	audit := &testAudit{}
	server, address := serveForTest(test, executor, audit, configuration, hostSigner)
	original := server.ConfigurationDigest()
	bad := configuration
	bad.Generation++
	bad.Target.TenantID = "another-tenant"
	if err := server.Activate(context.Background(), bad, hostSigner); !errors.Is(err, ErrConfiguration) {
		test.Fatal("cross-tenant target accepted", err)
	}
	bad = configuration
	bad.Generation++
	bad.ForwardRules = []ForwardRule{{Host: "169.254.169.254", Port: 80}}
	if err := server.Activate(context.Background(), bad, hostSigner); !errors.Is(err, ErrConfiguration) {
		test.Fatal("metadata forwarding accepted", err)
	}
	bad = configuration
	bad.MaxChannels++
	if err := server.Activate(context.Background(), bad, hostSigner); !errors.Is(err, ErrGeneration) {
		test.Fatal("same generation changed", err)
	}
	if server.ConfigurationDigest() != original {
		test.Fatal("invalid candidate changed serving state")
	}
	client := connectForTest(test, address, "app", hostSigner, userSigner)
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer session.Close()
	if err := session.Run("true"); err != nil {
		test.Fatal("previous state no longer serves", err)
	}
}

func TestManagedSSHForwardPolicyEnforcement(test *testing.T) {
	configuration, hostSigner, userSigner := testConfiguration(test)
	executor := &testExecutor{forward: func(ctx context.Context, target Target, host string, port uint32) (net.Conn, error) {
		client, backend := net.Pipe()
		go func() {
			defer backend.Close()
			buffer := make([]byte, 4)
			if _, err := io.ReadFull(backend, buffer); err == nil {
				_, _ = backend.Write(buffer)
			}
		}()
		return client, nil
	}}
	server, address := serveForTest(test, executor, &testAudit{}, configuration, hostSigner)
	client := connectForTest(test, address, "app", hostSigner, userSigner)
	if connection, err := client.Dial("tcp", "169.254.169.254:80"); err == nil {
		_ = connection.Close()
		test.Fatal("unapproved forwarding accepted")
	}
	forward, err := client.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		test.Fatal(err)
	}
	_, _ = forward.Write([]byte("ping"))
	buffer := make([]byte, 4)
	if _, err := io.ReadFull(forward, buffer); err != nil || string(buffer) != "ping" {
		test.Fatal("forward failed", err)
	}
	_ = forward.Close()
	configuration.Generation++
	configuration.ForwardRules = nil
	if err := server.Activate(context.Background(), configuration, hostSigner); err != nil {
		test.Fatal(err)
	}
	if connection, err := client.Dial("tcp", "127.0.0.1:9000"); err == nil {
		_ = connection.Close()
		test.Fatal("revoked forwarding accepted")
	}
}

func TestManagedSSHRevocationDoesNotDependOnRuntimeAvailability(test *testing.T) {
	configuration, hostSigner, _ := testConfiguration(test)
	executor := &testExecutor{}
	server, err := NewServer(executor, &testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), configuration, hostSigner); err != nil {
		test.Fatal(err)
	}
	executor.validateError = errors.New("runtime temporarily unavailable")
	configuration.Generation++
	configuration.Keys = nil
	if err := server.Activate(context.Background(), configuration, hostSigner); err != nil {
		test.Fatal("authorization revocation depended on workload health", err)
	}
	if server.Status().Generation != configuration.Generation {
		test.Fatal("revocation was not applied")
	}
	configuration.Generation++
	configuration.Target.PodUID = "replacement-pod"
	if err := server.Activate(context.Background(), configuration, hostSigner); err == nil {
		test.Fatal("new target accepted without runtime validation")
	}
}

func TestManagedSSHForwardRangeRemainsBoundToTargetLoopback(test *testing.T) {
	configuration, signer, _ := testConfiguration(test)
	configuration.ForwardRules = []ForwardRule{{Host: "127.0.0.1", Port: 40000, EndPort: 50000}}
	server, err := NewServer(&testExecutor{}, &testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), configuration, signer); err != nil {
		test.Fatal(err)
	}
	for _, port := range []uint32{40000, 45678, 50000} {
		if !forwardAllowed(configuration, "127.0.0.1", port) {
			test.Fatal("authorized dynamic port denied")
		}
	}
	for _, destination := range []struct {
		host string
		port uint32
	}{{"127.0.0.1", 39999}, {"127.0.0.1", 50001}, {"192.0.2.10", 45678}, {"169.254.169.254", 45678}} {
		if forwardAllowed(configuration, destination.host, destination.port) {
			test.Fatal("forward escaped configured target or range")
		}
	}
}
