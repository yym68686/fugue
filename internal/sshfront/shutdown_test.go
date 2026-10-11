package sshfront

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRunWaitsForEstablishedStreamsDuringShutdown(test *testing.T) {
	for _, force := range []bool{false, true} {
		name := "client_finishes"
		if force {
			name = "deadline_closes_remaining_stream"
		}
		test.Run(name, func(test *testing.T) {
			target, closeTarget := startEchoTarget(test)
			defer closeTarget()
			host, portText, _ := net.SplitHostPort(target)
			targetPort, _ := strconv.Atoi(portText)
			publicPort := freeTCPPort(test)
			route := model.EdgeSSHRoute{AppID: "workspace", TenantID: "tenant", PublicPort: publicPort, TargetHost: host, TargetPort: targetPort, Status: model.AppSSHEndpointStatusReady}
			bundle := model.EdgeSSHRouteBundle{Version: "shutdown-fixture", GeneratedAt: time.Now().UTC(), ValidUntil: time.Now().Add(time.Hour), Routes: []model.EdgeSSHRoute{route}}
			api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				_ = json.NewEncoder(writer).Encode(bundle)
			}))
			defer api.Close()
			configuration := Config{APIURL: api.URL, EdgeToken: "fixture-token", HealthAddr: "127.0.0.1:0", ListenHost: "127.0.0.1", PublicPortStart: publicPort, PublicPortEnd: publicPort, HTTPTimeout: time.Second, DialTimeout: time.Second, ShutdownTimeout: 500 * time.Millisecond}
			service := NewService(configuration, log.New(io.Discard, "", 0))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- service.Run(ctx) }()
			address := net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort))
			var connection net.Conn
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				var err error
				connection, err = net.DialTimeout("tcp", address, 50*time.Millisecond)
				if err == nil {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if connection == nil {
				test.Fatal("fixture never accepted a connection")
			}
			defer connection.Close()
			_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
			exchange := func() {
				test.Helper()
				if _, err := connection.Write([]byte("held")); err != nil {
					test.Fatal(err)
				}
				response := make([]byte, 4)
				if _, err := io.ReadFull(connection, response); err != nil || string(response) != "held" {
					test.Fatalf("held stream interrupted: %q %v", response, err)
				}
			}
			exchange()
			cancel()
			select {
			case err := <-finished:
				test.Fatalf("front exited with an established stream: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			exchange()
			unexpected, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
			if err == nil {
				unexpected.Close()
				test.Fatal("draining front admitted a new connection")
			}
			if !force {
				connection.Close()
			}
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					test.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				test.Fatal("shutdown exceeded its bound")
			}
			if force {
				buffer := make([]byte, 1)
				if _, err := connection.Read(buffer); err == nil {
					test.Fatal("shutdown deadline left the held stream open")
				} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
					test.Fatal("remaining connection was not closed at the shutdown deadline")
				}
			}
		})
	}
}
