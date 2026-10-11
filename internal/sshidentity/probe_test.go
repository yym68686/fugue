package sshidentity

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestProbeVerifiesIdentityWithoutUserAuthentication(test *testing.T) {
	keyring := testKeyring(test)
	record, err := keyring.Create("tenant_sample", "app_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	signer, err := keyring.Signer(record)
	if err != nil {
		test.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer listener.Close()
	var authenticationAttempts atomic.Int32
	configuration := &ssh.ServerConfig{
		PublicKeyCallback: func(metadata ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			authenticationAttempts.Add(1)
			return nil, errors.New("no probe authentication allowed")
		},
		AuthLogCallback: func(metadata ssh.ConnMetadata, method string, err error) { authenticationAttempts.Add(1) },
	}
	configuration.AddHostKey(signer)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		for attempt := 0; attempt < 2; attempt++ {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
			_, _, _, _ = ssh.NewServerConn(connection, configuration)
			_ = connection.Close()
		}
	}()
	result, err := Probe(context.Background(), listener.Addr().String(), signer.PublicKey(), 2*time.Second)
	if err != nil || !result.KeyExchangeVerified || !result.IdentityMatched || result.Fingerprint != record.Identity.Fingerprint {
		test.Fatal("matching identity was not verified", result, err)
	}
	other, err := keyring.Create("tenant_sample", "app_other", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	otherSigner, err := keyring.Signer(other)
	if err != nil {
		test.Fatal(err)
	}
	result, err = Probe(context.Background(), listener.Addr().String(), otherSigner.PublicKey(), 2*time.Second)
	if !errors.Is(err, ErrIdentityMismatch) || !result.KeyExchangeVerified || result.IdentityMatched || result.Fingerprint != record.Identity.Fingerprint {
		test.Fatal("wrong identity was not distinguished from connection failure", result, err)
	}
	<-completed
	if authenticationAttempts.Load() != 0 {
		test.Fatal("identity probe attempted user authentication")
	}
}

func TestProbeBoundsUnresponsiveHandshake(test *testing.T) {
	keyring := testKeyring(test)
	record, err := keyring.Create("tenant_sample", "app_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	signer, err := keyring.Signer(record)
	if err != nil {
		test.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer listener.Close()
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		buffer := make([]byte, 128)
		for {
			if _, err := connection.Read(buffer); err != nil {
				return
			}
		}
	}()
	started := time.Now()
	result, err := Probe(context.Background(), listener.Addr().String(), signer.PublicKey(), 75*time.Millisecond)
	if err == nil || result.KeyExchangeVerified || result.IdentityMatched || time.Since(started) > time.Second {
		test.Fatal("unresponsive peer was not bounded", result, err)
	}
	<-completed
}
