package edgequality

import (
	"context"
	"errors"
	"net/http/httptrace"
	"net/netip"
	"strings"
	"sync"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

type ClientProbeTarget struct {
	EdgeID  string `json:"edge_id"`
	Address string `json:"address"`
}

type ClientProbeObservation struct {
	Target            ClientProbeTarget `json:"target"`
	ObservedAt        time.Time         `json:"observed_at"`
	TCPAttempted      bool              `json:"tcp_attempted"`
	TCPEstablished    bool              `json:"tcp_established"`
	TCPConnectMS      *float64          `json:"tcp_connect_ms"`
	TCPConnectFailure *bool             `json:"tcp_connect_failure"`
	ProofVerified     bool              `json:"proof_verified"`
	Proof             *routeprobe.Proof `json:"proof,omitempty"`
	Failure           string            `json:"failure,omitempty"`
}

type ClientProbeCapture struct {
	Schema            string                   `json:"schema"`
	Hostname          string                   `json:"hostname"`
	Path              string                   `json:"path"`
	VantageLabel      string                   `json:"vantage_label"`
	Scope             string                   `json:"scope"`
	RoutingAuthorized bool                     `json:"routing_authorized"`
	StartedAt         time.Time                `json:"started_at"`
	FinishedAt        time.Time                `json:"finished_at"`
	Observations      []ClientProbeObservation `json:"observations"`
}

func CaptureClientPath(ctx context.Context, hostname, path, vantage string, targets []ClientProbeTarget, rounds int, interval, timeout time.Duration) (ClientProbeCapture, error) {
	if hostname == "" || len(hostname) > 253 || strings.ContainsAny(hostname, "/:@?# \t\r\n") || !strings.HasPrefix(path, "/") || len(path) > 2048 ||
		vantage == "" || len(vantage) > 128 || strings.ContainsAny(vantage, "\r\n\x00") || len(targets) == 0 || len(targets) > 8 || rounds < 1 || rounds > 3 || interval < 30*time.Second || interval > 5*time.Minute || timeout < time.Second || timeout > 5*time.Second {
		return ClientProbeCapture{}, errors.New("invalid bounded client probe request")
	}
	seen := map[string]bool{}
	for _, target := range targets {
		address, err := netip.ParseAddr(target.Address)
		if err != nil || !platformconfig.PublicDNSFlattenIP(address) || target.EdgeID == "" || len(target.EdgeID) > 128 || seen[target.EdgeID] || strings.ContainsAny(target.EdgeID, " \t\r\n\x00") {
			return ClientProbeCapture{}, errors.New("client probe requires unique physical edges and public IP addresses")
		}
		seen[target.EdgeID] = true
	}
	capture := ClientProbeCapture{Schema: "fugue.client-network-probe/v1", Hostname: hostname, Path: path, VantageLabel: vantage, Scope: "observer_local",
		StartedAt: time.Now().UTC(), Observations: []ClientProbeObservation{}}
	for round := 0; round < rounds; round++ {
		if round > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				capture.FinishedAt = time.Now().UTC()
				return capture, ctx.Err()
			case <-timer.C:
			}
		}
		for _, target := range targets {
			if err := ctx.Err(); err != nil {
				capture.FinishedAt = time.Now().UTC()
				return capture, err
			}
			capture.Observations = append(capture.Observations, measureClientPath(ctx, hostname, path, target, timeout, routeprobe.Probe))
		}
	}
	capture.FinishedAt = time.Now().UTC()
	return capture, nil
}

func measureClientPath(ctx context.Context, hostname, path string, target ClientProbeTarget, timeout time.Duration, probe func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error)) ClientProbeObservation {
	observation := ClientProbeObservation{Target: target, ObservedAt: time.Now().UTC()}
	var mutex sync.Mutex
	var started time.Time
	connections := 0
	trace := &httptrace.ClientTrace{
		ConnectStart: func(network, address string) {
			mutex.Lock()
			defer mutex.Unlock()
			connections++
			started = time.Now()
			observation.TCPAttempted = true
		},
		ConnectDone: func(network, address string, err error) {
			mutex.Lock()
			defer mutex.Unlock()
			if !started.IsZero() && connections == 1 {
				failed := err != nil
				observation.TCPConnectFailure = &failed
			}
			if err == nil && !started.IsZero() && connections == 1 {
				elapsed := float64(time.Since(started)) / float64(time.Millisecond)
				observation.TCPConnectMS = &elapsed
				observation.TCPEstablished = true
			}
		},
	}
	proof, err := probe(httptrace.WithClientTrace(ctx, trace), hostname, path, target.Address, "", timeout)
	mutex.Lock()
	defer mutex.Unlock()
	if connections != 1 {
		observation.TCPConnectMS = nil
		observation.TCPConnectFailure = nil
		observation.TCPEstablished = false
	}
	switch {
	case err != nil:
		observation.Failure = "tls_or_route_proof_failed"
		if observation.TCPAttempted && !observation.TCPEstablished {
			observation.Failure = "tcp_connect_not_established"
		}
	case proof.EdgeID != target.EdgeID:
		observation.Failure = "physical_edge_identity_mismatch"
	default:
		observation.ProofVerified = true
		observation.Proof = &proof
	}
	return observation
}
