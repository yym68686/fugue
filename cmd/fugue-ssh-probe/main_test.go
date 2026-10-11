package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/sshidentity"
	"golang.org/x/crypto/ssh"
)

func TestProbeCommandRequiresExactBoundedPublicIdentity(test *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		test.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		test.Fatal(err)
	}
	key, _ := ssh.NewPublicKey(public)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer listener.Close()
	var authentications atomic.Int32
	configuration := &ssh.ServerConfig{PublicKeyCallback: func(metadata ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		authentications.Add(1)
		return nil, nil
	}}
	configuration.AddHostKey(signer)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		server, _, _, _ := ssh.NewServerConn(connection, configuration)
		if server != nil {
			server.Close()
		}
	}()
	request := map[string]string{"target": listener.Addr().String(), "public_key": strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key))), "fingerprint": ssh.FingerprintSHA256(key)}
	raw, _ := json.Marshal(request)
	var output bytes.Buffer
	if err := run(context.Background(), bytes.NewReader(raw), &output); err != nil {
		test.Fatal(err)
	}
	var observation sshidentity.Observation
	if json.Unmarshal(output.Bytes(), &observation) != nil || !observation.IdentityMatched || !observation.KeyExchangeVerified {
		test.Fatal("command did not prove host identity", output.String())
	}
	<-completed
	if authentications.Load() != 0 {
		test.Fatal("host probe attempted user authentication")
	}
	for _, invalid := range []string{string(raw) + "{}", string(raw) + strings.Repeat(" ", 8193), `{"private_key":"not-accepted"}`, `{"target":"invalid","public_key":"invalid","fingerprint":"invalid"}`} {
		if err := run(context.Background(), strings.NewReader(invalid), io.Discard); err == nil {
			test.Fatal("invalid or oversized probe accepted")
		}
	}
}
