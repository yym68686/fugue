package sshfront

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAcknowledgementRequiresDurableExactListenersAndCountsOldStreams(test *testing.T) {
	first, closeFirst := startEchoTarget(test)
	defer closeFirst()
	second, closeSecond := startEchoTarget(test)
	defer closeSecond()
	firstHost, firstPortText, _ := net.SplitHostPort(first)
	_, secondPortText, _ := net.SplitHostPort(second)
	firstPort, _ := strconv.Atoi(firstPortText)
	secondPort, _ := strconv.Atoi(secondPortText)
	publicPort := freeTCPPort(test)
	route := model.EdgeSSHRoute{AppID: "workspace", TenantID: "tenant", PublicPort: publicPort, TargetHost: firstHost, TargetPort: firstPort, Status: model.AppSSHEndpointStatusReady}
	var mutex sync.Mutex
	bundle := model.EdgeSSHRouteBundle{Version: "first", GeneratedAt: time.Now().UTC(), ValidUntil: time.Now().Add(time.Hour), Routes: []model.EdgeSSHRoute{route}}
	acks := []model.EdgeSSHAcknowledgementRequest{}
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		if request.URL.Path == "/v1/edge/ssh/routes" {
			_ = json.NewEncoder(writer).Encode(bundle)
			return
		}
		if request.URL.Path != "/v1/edge/ssh/ack" || request.Header.Get("Authorization") != "Bearer edge-token" || request.URL.Query().Get("token") != "" {
			test.Error("incorrect acknowledgement authentication or URL")
			writer.WriteHeader(403)
			return
		}
		var ack model.EdgeSSHAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&ack); err != nil {
			test.Error(err)
		}
		acks = append(acks, ack)
		_, _ = io.WriteString(writer, `{"accepted":true}`)
	}))
	defer api.Close()
	configuration := Config{APIURL: api.URL, EdgeToken: "edge-token", EdgeID: "edge-a", EdgeGroupID: "group-a", InstanceUID: "pod-a", CachePath: filepath.Join(test.TempDir(), "routes.json"), ListenHost: "127.0.0.1", PublicPortStart: publicPort, PublicPortEnd: publicPort, DialTimeout: time.Second, HTTPTimeout: time.Second}
	service := NewService(configuration, log.New(io.Discard, "", 0))
	defer service.closeAllListeners()
	service.syncAndLog(context.Background(), configuration)
	connection, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := connection.Write([]byte("hello")); err != nil {
		test.Fatal(err)
	}
	response := make([]byte, 5)
	if _, err := io.ReadFull(connection, response); err != nil {
		test.Fatal(err)
	}
	mutex.Lock()
	bundle.Version = "second"
	bundle.Routes[0].TargetPort = secondPort
	mutex.Unlock()
	blocked := filepath.Join(test.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("fixture"), 0o600); err != nil {
		test.Fatal(err)
	}
	bad := configuration
	bad.CachePath = filepath.Join(blocked, "routes.json")
	service.syncAndLog(context.Background(), bad)
	service.mu.Lock()
	if service.bundle.Version != "first" || service.listeners[publicPort].route.TargetPort != firstPort {
		test.Fatal("failed persistence replaced positive serving state")
	}
	service.mu.Unlock()
	service.syncAndLog(context.Background(), configuration)
	mutex.Lock()
	defer mutex.Unlock()
	if len(acks) != 2 || acks[1].BundleVersion != "second" || !acks[1].Persisted || len(acks[1].Connections) != 1 || acks[1].Connections[0].TargetPort != firstPort || acks[1].Connections[0].Count != 1 {
		test.Fatal("acknowledgement lost old established stream or acknowledged failure", acks)
	}
	raw, err := os.ReadFile(configuration.CachePath)
	if err != nil {
		test.Fatal(err)
	}
	var cached model.EdgeSSHRouteBundle
	if json.Unmarshal(raw, &cached) != nil || cached.Version != "second" {
		test.Fatal("acknowledged cache differs from loaded route")
	}
}
