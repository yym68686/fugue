package originhealth

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestProbeEndpointTCPAndBuildRecord(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatal(err)
	}
	probe := ProbeEndpointTCP(context.Background(), model.EndpointLKGEndpoint{
		IP:    "127.0.0.1",
		Port:  port,
		Ready: true,
	}, time.Second)
	if probe.Status != ProbeStatusPass {
		t.Fatalf("expected pass, got %#v", probe)
	}
	record := BuildRecord(RecordInput{
		Hostname:        "api.example.com",
		PathPrefix:      "/v1",
		RouteGeneration: "route-1",
		ServiceIdentity: "tenant|app|runtime",
		EndpointIPProbe: &probe,
		CheckedAt:       time.Unix(1700000000, 0).UTC(),
	})
	if record.Hostname != "api.example.com" || record.PathPrefix != "/v1" {
		t.Fatalf("unexpected record identity: %#v", record)
	}
	if record.Status != ProbeStatusPass || record.EndpointIPProbe == nil {
		t.Fatalf("unexpected record status: %#v", record)
	}
}

func TestProbeHTTPClassifiesServerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()
	probe := ProbeHTTP(context.Background(), server.URL, time.Second, false)
	if probe.Status != ProbeStatusFail {
		t.Fatalf("expected failed HTTP probe, got %#v", probe)
	}
	if probe.Evidence["failure_class"] != "http_probe_5xx" {
		t.Fatalf("expected http_probe_5xx evidence, got %#v", probe.Evidence)
	}
	record := BuildRecord(RecordInput{
		Hostname:  "api.example.com",
		HTTPProbe: &probe,
	})
	if record.Status != ProbeStatusFail || record.LastFailureClass != "http_probe_5xx" {
		t.Fatalf("expected failed record with failure class, got %#v", record)
	}
}
