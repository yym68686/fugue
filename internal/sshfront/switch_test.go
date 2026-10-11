package sshfront

import (
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestTargetSwitchRetainsListenerAndEstablishedConnection(test *testing.T) {
	firstAddress, closeFirst := startEchoTarget(test)
	defer closeFirst()
	second, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer second.Close()
	go func() {
		connection, acceptError := second.Accept()
		if acceptError == nil {
			defer connection.Close()
			_, _ = connection.Write([]byte("new"))
		}
	}()
	firstHost, firstPort, _ := net.SplitHostPort(firstAddress)
	portNumber, _ := strconv.Atoi(firstPort)
	publicPort := freeTCPPort(test)
	configuration := Config{ListenHost: "127.0.0.1", PublicPortStart: publicPort, PublicPortEnd: publicPort, DialTimeout: time.Second}
	service := NewService(configuration, log.New(io.Discard, "", 0))
	defer service.closeAllListeners()
	route := model.EdgeSSHRoute{AppID: "workspace", TenantID: "tenant", PublicPort: publicPort, TargetHost: firstHost, TargetPort: portNumber, Status: model.AppSSHEndpointStatusReady}
	if err := service.reconcile(configuration, []model.EdgeSSHRoute{route}); err != nil {
		test.Fatal(err)
	}
	listener := service.listeners[publicPort].listener
	connection, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	assertExchange := func() {
		test.Helper()
		if _, err := connection.Write([]byte("old")); err != nil {
			test.Fatal(err)
		}
		response := make([]byte, 3)
		if _, err := io.ReadFull(connection, response); err != nil || string(response) != "old" {
			test.Fatalf("old connection disrupted: %q %v", response, err)
		}
	}
	assertExchange()
	route.TargetPort = second.Addr().(*net.TCPAddr).Port
	route.RouteGeneration = "next"
	if err := service.reconcile(configuration, []model.EdgeSSHRoute{route}); err != nil {
		test.Fatal(err)
	}
	if service.listeners[publicPort].listener != listener {
		test.Fatal("target switch rebound public listener")
	}
	assertExchange()
	newConnection, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer newConnection.Close()
	_ = newConnection.SetReadDeadline(time.Now().Add(5 * time.Second))
	response := make([]byte, 3)
	if _, err := io.ReadFull(newConnection, response); err != nil || string(response) != "new" {
		test.Fatalf("new connection missed new target: %q %v", response, err)
	}
}

func TestRejectedCandidatePreservesAllServingRoutes(test *testing.T) {
	target, closeTarget := startEchoTarget(test)
	defer closeTarget()
	host, port, _ := net.SplitHostPort(target)
	portNumber, _ := strconv.Atoi(port)
	publicPort := freeTCPPort(test)
	configuration := Config{ListenHost: "127.0.0.1", PublicPortStart: 1, PublicPortEnd: 65535, DialTimeout: time.Second}
	service := NewService(configuration, log.New(io.Discard, "", 0))
	defer service.closeAllListeners()
	route := model.EdgeSSHRoute{AppID: "workspace", TenantID: "tenant", PublicPort: publicPort, TargetHost: host, TargetPort: portNumber, Status: model.AppSSHEndpointStatusReady}
	if err := service.reconcile(configuration, []model.EdgeSSHRoute{route}); err != nil {
		test.Fatal(err)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	defer occupied.Close()
	candidate := route
	candidate.PublicPort = occupied.Addr().(*net.TCPAddr).Port
	if err := service.reconcile(configuration, []model.EdgeSSHRoute{candidate}); err == nil {
		test.Fatal("occupied candidate accepted")
	}
	assertProxyEcho(test, publicPort, "still-serving")
	if err := service.reconcile(configuration, []model.EdgeSSHRoute{route, route}); err == nil {
		test.Fatal("ambiguous port accepted")
	}
	assertProxyEcho(test, publicPort, "still-serving")
}
