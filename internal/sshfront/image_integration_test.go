package sshfront

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func TestActualSSHFrontImage(test *testing.T) {
	image := os.Getenv("FUGUE_SSH_FRONT_TEST_IMAGE")
	if image == "" {
		test.Skip("requires an explicitly verified immutable front image")
	}
	if !regexp.MustCompile(`^(?:[^\s]+@)?sha256:[a-f0-9]{64}$`).MatchString(image) {
		test.Fatal("immutable front image required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	docker := func(arguments ...string) []byte {
		test.Helper()
		output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
		if err != nil {
			test.Fatalf("isolated image command failed: %v: %s", err, output)
		}
		return output
	}
	backend := func(identity string) int {
		listener, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			test.Fatal(err)
		}
		test.Cleanup(func() { _ = listener.Close() })
		go func() {
			for {
				connection, err := listener.Accept()
				if err != nil {
					return
				}
				go func() {
					defer connection.Close()
					_ = connection.SetDeadline(time.Now().Add(90 * time.Second))
					scanner := bufio.NewScanner(connection)
					for scanner.Scan() {
						_, _ = fmt.Fprintln(connection, identity+":"+scanner.Text())
					}
				}()
			}
		}()
		return listener.Addr().(*net.TCPAddr).Port
	}
	firstPort, secondPort := backend("previous"), backend("candidate")
	route := model.EdgeSSHRoute{AppID: "workspace", TenantID: "tenant", PublicPort: 29444, TargetHost: "host.docker.internal", TargetPort: firstPort, Status: model.AppSSHEndpointStatusReady}
	bundle := bundleauth.SignEdgeSSHRouteBundle(model.EdgeSSHRouteBundle{Version: "first", GeneratedAt: time.Now().UTC(), Routes: []model.EdgeSSHRoute{route}}, "fixture-signing-key", "fixture", 5*time.Minute)
	var mutex sync.Mutex
	var lastAck model.EdgeSSHAcknowledgementRequest
	api := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer fixture-edge-token" || request.URL.Query().Get("token") != "" {
			test.Error("front image leaked credential into query or omitted bearer authentication")
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		mutex.Lock()
		defer mutex.Unlock()
		switch request.URL.Path {
		case "/v1/edge/ssh/routes":
			_ = json.NewEncoder(writer).Encode(bundle)
		case "/v1/edge/ssh/ack":
			if json.NewDecoder(request.Body).Decode(&lastAck) != nil || !lastAck.Persisted || lastAck.EdgeID != "edge-fixture" || lastAck.EdgeGroupID != "group-fixture" || lastAck.InstanceUID != "pod-fixture" {
				test.Error("actual image omitted exact durable acknowledgement")
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(writer, `{"accepted":true}`)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	})}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		test.Fatal(err)
	}
	go func() { _ = api.Serve(listener) }()
	defer api.Close()
	name := "fugue-ssh-front-test-" + strconv.FormatInt(time.Now().UnixNano(), 16)
	cache := test.TempDir()
	docker("run", "-d", "--name", name, "--label", "fugue.test.ssh-front=true", "--add-host", "host.docker.internal:host-gateway", "-p", "127.0.0.1::29444", "-p", "127.0.0.1::7836", "-v", cache+":/state", "-e", "FUGUE_API_URL=http://host.docker.internal:"+strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)+"?token=must-not-forward", "-e", "FUGUE_EDGE_TOKEN=fixture-edge-token", "-e", "FUGUE_SSH_FRONT_EDGE_ID=edge-fixture", "-e", "FUGUE_SSH_FRONT_EDGE_GROUP_ID=group-fixture", "-e", "FUGUE_SSH_FRONT_INSTANCE_UID=pod-fixture", "-e", "FUGUE_SSH_PUBLIC_PORT_START=29444", "-e", "FUGUE_SSH_PUBLIC_PORT_END=29444", "-e", "FUGUE_SSH_FRONT_HEALTH_LISTEN_ADDR=:7836", "-e", "FUGUE_SSH_FRONT_ROUTES_CACHE_PATH=/state/routes.json", "-e", "FUGUE_SSH_FRONT_SYNC_INTERVAL=200ms", "-e", "FUGUE_BUNDLE_SIGNING_KEY=fixture-signing-key", "-e", "FUGUE_BUNDLE_SIGNING_KEY_ID=fixture", image)
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if test.Failed() {
			output, _ := exec.CommandContext(cleanup, "docker", "logs", "--tail", "30", name).CombinedOutput()
			test.Logf("actual image diagnostics: %s", output)
		}
		_ = exec.CommandContext(cleanup, "docker", "rm", "-f", name).Run()
	}()
	endpoint := strings.TrimSpace(string(docker("port", name, "29444/tcp")))
	health := strings.TrimSpace(string(docker("port", name, "7836/tcp")))
	wait := func(description string, condition func() bool) {
		test.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for !condition() {
			if time.Now().After(deadline) {
				test.Fatal(description)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	wait("initial image publication did not ACK", func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		return lastAck.BundleVersion == "first"
	})
	connect := func() net.Conn {
		test.Helper()
		var connection net.Conn
		var err error
		deadline := time.Now().Add(5 * time.Second)
		for {
			connection, err = net.DialTimeout("tcp", endpoint, time.Second)
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			test.Fatal(err)
		}
		test.Cleanup(func() { _ = connection.Close() })
		return connection
	}
	exchange := func(connection net.Conn, expected string) {
		test.Helper()
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		_, err := io.WriteString(connection, "witness\n")
		if err != nil {
			test.Fatal(err)
		}
		value, err := bufio.NewReader(connection).ReadString('\n')
		if err != nil || value != expected+":witness\n" {
			test.Fatal("actual front lost target or established stream", value, err)
		}
	}
	previous := connect()
	exchange(previous, "previous")
	mutex.Lock()
	route.TargetPort = secondPort
	bundle = bundleauth.SignEdgeSSHRouteBundle(model.EdgeSSHRouteBundle{Version: "second", GeneratedAt: time.Now().UTC(), Routes: []model.EdgeSSHRoute{route}}, "fixture-signing-key", "fixture", 5*time.Minute)
	mutex.Unlock()
	wait("actual image did not retain previous-target connection in ACK", func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		for _, connection := range lastAck.Connections {
			if lastAck.BundleVersion == "second" && lastAck.RouteDigest == model.SSHRoutesDigest(bundle.Routes) && connection.TargetPort == firstPort && connection.Count == 1 {
				return true
			}
		}
		return false
	})
	candidate := connect()
	exchange(previous, "previous")
	exchange(candidate, "candidate")
	if mode := strings.TrimSpace(string(docker("exec", name, "stat", "-c", "%a", "/state/routes.json"))); mode != "600" {
		test.Fatal("actual route cache lost its private permissions", mode)
	}
	raw := docker("exec", name, "cat", "/state/routes.json")
	var persisted model.EdgeSSHRouteBundle
	if json.Unmarshal(raw, &persisted) != nil || persisted.Version != "second" {
		test.Fatal("actual image acknowledged a non-durable selection")
	}
	mutex.Lock()
	bundle.Version = "unsigned-change"
	mutex.Unlock()
	client := &http.Client{Timeout: time.Second}
	wait("actual image did not report rejected signature while retaining LKG", func() bool {
		response, err := client.Get("http://" + health + "/healthz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var status struct {
			Version string `json:"bundle_version"`
			Error   string `json:"last_error"`
		}
		return json.NewDecoder(response.Body).Decode(&status) == nil && status.Version == "second" && status.Error != ""
	})
	exchange(previous, "previous")
	exchange(candidate, "candidate")
	exchange(connect(), "candidate")
	test.Log("actual front image verified signed routes, exact durable ACKs, preserved connections, rejected configuration, and credential privacy")
}
