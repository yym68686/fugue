package edge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
	"fugue/internal/routeproof"
)

const candidateExecutionSchema = "fugue.edge.candidate-execution/v1"
const candidateExecutionHeader = "X-Fugue-Candidate-Execution"
const candidateRouteHeader = "X-Fugue-Candidate-Route"

// This receipt attests a completed isolated execution, never a serving LKG.
type PlatformCandidateExecution struct {
	Schema                string    `json:"schema"`
	Mode                  string    `json:"mode"`
	Result                string    `json:"result"`
	ArtifactID            string    `json:"artifact_id"`
	ArtifactDigest        string    `json:"artifact_digest"`
	ArtifactGeneration    string    `json:"artifact_generation"`
	NodeID                string    `json:"node_id"`
	EdgeGroupID           string    `json:"edge_group_id"`
	ReleaseSetID          string    `json:"release_set_id"`
	ExpectedConsumerSetID string    `json:"expected_consumer_set_id"`
	GenerationSequence    int64     `json:"generation_sequence"`
	FencingToken          int64     `json:"fencing_token"`
	RouteIndexDigest      string    `json:"route_index_digest"`
	CaddyConfigDigest     string    `json:"caddy_config_digest"`
	ProbeCount            int       `json:"probe_count"`
	ObservedAt            time.Time `json:"observed_at"`
	ExpiresAt             time.Time `json:"expires_at"`
	Serving               bool      `json:"serving"`
	TLSVerified           bool      `json:"tls_verified"`
	OriginVerified        bool      `json:"origin_verified"`
	ReceiptDigest         string    `json:"receipt_digest,omitempty"`
}

func (receipt PlatformCandidateExecution) digest() (string, error) {
	receipt.ReceiptDigest = ""
	return platformconfig.Digest(receipt)
}

func (receipt PlatformCandidateExecution) matches(artifact model.PlatformArtifact, assignment model.PlatformConsumerAssignment, nodeID, groupID, indexDigest string, now time.Time) bool {
	digest, err := receipt.digest()
	return err == nil && receipt.Schema == candidateExecutionSchema && receipt.Mode == "isolated_http" && receipt.Result == "passed" &&
		receipt.ReceiptDigest == digest && receipt.ArtifactID == artifact.ID && receipt.ArtifactDigest == artifact.ContentHash &&
		receipt.ArtifactGeneration == artifact.Generation && receipt.ReleaseSetID == assignment.ReleaseSetID &&
		receipt.ExpectedConsumerSetID == assignment.ExpectedConsumerSetID && receipt.GenerationSequence == assignment.GenerationSequence &&
		receipt.FencingToken == assignment.FencingToken && receipt.RouteIndexDigest == indexDigest && receipt.ProbeCount > 0 &&
		receipt.NodeID == nodeID && receipt.EdgeGroupID == groupID && receipt.CaddyConfigDigest != "" &&
		!receipt.ObservedAt.IsZero() && receipt.ObservedAt.Before(now.Add(time.Second)) && receipt.ExpiresAt.After(now.Add(30*time.Second)) &&
		receipt.ExpiresAt.Sub(receipt.ObservedAt) <= 2*time.Minute &&
		!receipt.Serving && !receipt.TLSVerified && !receipt.OriginVerified
}

func (s *Service) executePlatformCandidate(ctx context.Context, artifact model.PlatformArtifact, assignment model.PlatformConsumerAssignment, indexDigest string) (*PlatformCandidateExecution, error) {
	if !s.Config.CaddyEnabled {
		return nil, nil
	}
	// Only this process's completed execution can be reused. A restart must
	// execute again; a persisted receipt is not fresh runtime evidence.
	previous := s.Status().PlatformCandidate.Execution
	if previous != nil && previous.matches(artifact, assignment, s.Config.EdgeID, s.Config.EdgeGroupID, indexDigest, time.Now().UTC()) {
		return previous, nil
	}
	bundle, err := routeartifact.MaterializeForGroup(artifact, s.Config.EdgeGroupID)
	if err != nil {
		return nil, err
	}
	configDigest, probes, err := runIsolatedCandidate(ctx, bundle, filepath.Dir(s.Config.CachePath), "/usr/local/bin/caddy")
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	receipt := &PlatformCandidateExecution{
		Schema: candidateExecutionSchema, Mode: "isolated_http", Result: "passed",
		ArtifactID: artifact.ID, ArtifactDigest: artifact.ContentHash,
		ArtifactGeneration: artifact.Generation, NodeID: s.Config.EdgeID, EdgeGroupID: s.Config.EdgeGroupID,
		ReleaseSetID: assignment.ReleaseSetID, ExpectedConsumerSetID: assignment.ExpectedConsumerSetID,
		GenerationSequence: assignment.GenerationSequence, FencingToken: assignment.FencingToken,
		RouteIndexDigest: indexDigest, CaddyConfigDigest: configDigest, ProbeCount: probes,
		ObservedAt: now, ExpiresAt: now.Add(2 * time.Minute),
	}
	receipt.ReceiptDigest, err = receipt.digest()
	return receipt, err
}

func candidateHTTPConfig(bundle model.EdgeRouteBundle, listener, proxy string) ([]byte, error) {
	isolated := &Service{Config: config.EdgeConfig{
		EdgeGroupID: bundle.EdgeGroupID, CaddyListenAddr: listener,
		CaddyAdminURL: "http://127.0.0.1:2019", CaddyProxyListenAddr: proxy, CaddyTLSMode: caddyTLSModeOff,
	}}
	raw, _, err := isolated.buildCaddyConfig(bundle)
	if err != nil {
		return nil, err
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	document["admin"] = map[string]any{"disabled": true, "config": map[string]any{"persist": false}}
	// Retain the generated logger identity without recording candidate hostnames.
	document["logging"] = map[string]any{"logs": map[string]any{
		"default":           map[string]any{"writer": map[string]string{"output": "discard"}},
		"fugue_edge_access": map[string]any{"writer": map[string]string{"output": "discard"}, "include": []string{"http.log.access.fugue_edge_access"}},
	}}
	return json.Marshal(document)
}

// The candidate backend has no origin transport or reference to Service's
// serving pointer. Only a per-execution nonce can obtain candidate proofs.
func candidateProofHandler(index *edgeRouteIndex, nonce string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || len(r.Header.Values(candidateExecutionHeader)) != 1 || r.Header.Get(candidateExecutionHeader) != nonce {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		route, ok, _, _, _ := index.routeForRequest(normalizeRouteHost(r.Host), r.URL.Path)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		digest, err := routeproof.Digest(route)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set(candidateExecutionHeader, nonce)
		w.Header().Set(candidateRouteHeader, digest)
		if route.Status != model.EdgeRouteStatusActive {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func runIsolatedCandidate(parent context.Context, bundle model.EdgeRouteBundle, directory, binary string) (string, int, error) {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	if len(bundle.Routes) > 10000 {
		return "", 0, errors.New("candidate execution route limit exceeded")
	}
	index := buildEdgeRouteIndex(bundle, bundle.EdgeGroupID, routePublicationMetadata{Candidate: true})
	if err := probePlatformCandidateIndex(bundle, index); err != nil {
		return "", 0, err
	}
	eligible := 0
	for _, route := range bundle.Routes {
		if model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			eligible++
		}
	}
	if eligible == 0 {
		return "", 0, errors.New("candidate execution has no probeable routes")
	}
	workspace, err := os.MkdirTemp(directory, ".candidate-exec-")
	if err != nil {
		return "", 0, errors.New("candidate execution workspace unavailable")
	}
	defer os.RemoveAll(workspace)
	nonceBytes := make([]byte, 24)
	if _, err := rand.Read(nonceBytes); err != nil {
		return "", 0, err
	}
	nonce := hex.EncodeToString(nonceBytes)
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return "", 0, errors.New("candidate backend listener unavailable")
	}
	server := &http.Server{Handler: candidateProofHandler(index, nonce), ReadHeaderTimeout: time.Second}
	serverDone := make(chan struct{})
	go func() { defer close(serverDone); _ = server.Serve(backend) }()
	defer func() { _ = server.Close(); <-serverDone }()
	reserved, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return "", 0, errors.New("candidate Caddy listener unavailable")
	}
	address := reserved.Addr().String()
	raw, err := candidateHTTPConfig(bundle, address, backend.Addr().String())
	_ = reserved.Close()
	if err != nil {
		return "", 0, errors.New("candidate Caddy configuration invalid")
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		return "", 0, err
	}
	configDigest, err := platformconfig.Digest(document)
	if err != nil {
		return "", 0, err
	}
	cmd := exec.CommandContext(ctx, binary, "run", "--config", "-")
	cmd.Dir = workspace
	// Do not pass API credentials, proxy settings or signing keys to Caddy.
	cmd.Env = []string{"XDG_DATA_HOME=" + workspace, "XDG_CONFIG_HOME=" + workspace, "GOMAXPROCS=1"}
	cmd.Stdin = bytes.NewReader(raw)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		return "", 0, errors.New("candidate Caddy start failed")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		cancel()
		// Always reap the process, including cancellation and failed probes.
		<-done
	}()
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	probed := 0
	for _, route := range bundle.Routes {
		if !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			continue
		}
		expected, err := routeproof.Digest(route)
		if err != nil {
			return "", 0, err
		}
		path := model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		endpoint := (&url.URL{Scheme: "http", Host: address, Path: path}).String()
		for {
			request, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
			if err != nil {
				return "", 0, err
			}
			request.Host = route.Hostname
			request.Header.Set(candidateExecutionHeader, nonce)
			response, requestErr := client.Do(request)
			if requestErr == nil {
				_ = response.Body.Close()
				wantStatus := http.StatusNoContent
				if route.Status != model.EdgeRouteStatusActive {
					wantStatus = http.StatusServiceUnavailable
				}
				if response.StatusCode != wantStatus || response.Header.Get(candidateExecutionHeader) != nonce || response.Header.Get(candidateRouteHeader) != expected {
					return "", 0, errors.New("candidate Caddy route probe rejected: HTTP " + strconv.Itoa(response.StatusCode))
				}
				probed++
				break
			}
			// Retry connection setup while Caddy starts, but never retry a
			// completed HTTP response whose route proof is wrong.
			select {
			case err := <-done:
				done <- err
				return "", 0, errors.New("candidate Caddy exited before probe completion")
			case <-ctx.Done():
				return "", 0, errors.New("candidate Caddy probe deadline exceeded")
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	if ctx.Err() != nil {
		return "", 0, errors.New("candidate Caddy execution cancelled")
	}
	return configDigest, probed, nil
}
