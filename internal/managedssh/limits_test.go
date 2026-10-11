package managedssh

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestConnectionAndChannelAdmissionLimitsReleaseCapacity(test *testing.T) {
	configuration, host, user := testConfiguration(test)
	configuration.MaxConnections, configuration.MaxConnectionsPerIP, configuration.MaxChannels = 2, 1, 1
	server, address := serveForTest(test, &testExecutor{}, &testAudit{}, configuration, host)
	client := connectForTest(test, address, "app", host, user)
	options := &ssh.ClientConfig{User: "app", Auth: []ssh.AuthMethod{ssh.PublicKeys(user)}, HostKeyCallback: ssh.FixedHostKey(host.PublicKey()), Timeout: time.Second}
	if extra, err := ssh.Dial("tcp", address, options); err == nil {
		extra.Close()
		test.Fatal("per-source connection limit admitted another connection")
	}
	first, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer first.Close()
	if extra, err := client.NewSession(); err == nil {
		extra.Close()
		test.Fatal("per-connection channel limit admitted another session")
	} else {
		var rejected *ssh.OpenChannelError
		if !errors.As(err, &rejected) || rejected.Reason != ssh.ResourceShortage {
			test.Fatal("channel admission failed for an unrelated reason", err)
		}
	}
	client.Close()
	deadline := time.Now().Add(2 * time.Second)
	for server.Status().ActiveConnections != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if server.Status().ActiveConnections != 0 {
		test.Fatal("disconnected transport retained capacity")
	}
	replacement := connectForTest(test, address, "app", host, user)
	session, err := replacement.NewSession()
	if err != nil {
		test.Fatal("released capacity was not reusable", err)
	}
	session.Close()
}

func TestAuthorizationExpiryClosesHeldSessionWithoutConfigurationRefresh(test *testing.T) {
	configuration, host, user := testConfiguration(test)
	configuration.Keys[0].ExpiresAt = time.Now().Add(2 * time.Second)
	started, stopped := make(chan struct{}), make(chan struct{})
	executor := &testExecutor{run: func(ctx context.Context, target Target, request SessionRequest, streams SessionIO) (Exit, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return Exit{}, ctx.Err()
	}}
	_, address := serveForTest(test, executor, &testAudit{}, configuration, host)
	client := connectForTest(test, address, "app", host, user)
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer session.Close()
	if err := session.Start("held-command"); err != nil {
		test.Fatal(err)
	}
	<-started
	select {
	case <-stopped:
	case <-time.After(4 * time.Second):
		test.Fatal("expired grant retained an active command")
	}
	if err := session.Wait(); err == nil {
		test.Fatal("expired session completed as if still authorized")
	}
	options := &ssh.ClientConfig{User: "app", Auth: []ssh.AuthMethod{ssh.PublicKeys(user)}, HostKeyCallback: ssh.FixedHostKey(host.PublicKey()), Timeout: time.Second}
	if connection, err := ssh.Dial("tcp", address, options); err == nil {
		connection.Close()
		test.Fatal("expired user grant authenticated a new connection")
	}
}

func TestAuditFailurePreventsExecutionOnAlreadyAuthenticatedConnection(test *testing.T) {
	configuration, host, user := testConfiguration(test)
	var executions atomic.Int32
	executor := &testExecutor{run: func(context.Context, Target, SessionRequest, SessionIO) (Exit, error) {
		executions.Add(1)
		return Exit{}, nil
	}}
	audit := &testAudit{}
	_, address := serveForTest(test, executor, audit, configuration, host)
	client := connectForTest(test, address, "app", host, user)
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer session.Close()
	audit.mu.Lock()
	audit.failure = true
	audit.mu.Unlock()
	if err := session.Run("must-not-execute"); err == nil || executions.Load() != 0 {
		test.Fatal("audit storage failure admitted an unrecorded command", err)
	}
}
