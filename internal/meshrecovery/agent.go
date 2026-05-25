package meshrecovery

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

type CommandExecutor interface {
	Run(ctx context.Context, name string, args ...string) error
}

type OSCommandExecutor struct{}

func (OSCommandExecutor) Run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
}

type MeshAgent struct {
	cfg      MeshAgentConfig
	logger   *log.Logger
	client   *http.Client
	executor CommandExecutor
	now      func() time.Time
}

type heartbeatResult struct {
	endpoint string
	response HeartbeatResponse
}

type agentState struct {
	Generation        string    `json:"generation,omitempty"`
	LastSyncAt        time.Time `json:"last_sync_at,omitempty"`
	LastRejoinAt      time.Time `json:"last_rejoin_at,omitempty"`
	LastEndpoint      string    `json:"last_endpoint,omitempty"`
	LastDirectoryHash string    `json:"last_directory_hash,omitempty"`
}

func NewMeshAgent(cfg MeshAgentConfig, logger *log.Logger) (*MeshAgent, error) {
	cfg.Endpoints = cleanList(cfg.Endpoints)
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("at least one mesh recovery endpoint is required")
	}
	cfg.SigningKey = strings.TrimSpace(cfg.SigningKey)
	if cfg.SigningKey == "" {
		return nil, ErrMissingSigningKey
	}
	cfg.SigningKeyID = strings.TrimSpace(cfg.SigningKeyID)
	cfg.Token = strings.TrimSpace(cfg.Token)
	cfg.Node.NodeID = strings.TrimSpace(cfg.Node.NodeID)
	if cfg.Node.NodeID == "" {
		return nil, fmt.Errorf("mesh agent node_id is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 10 * time.Second
	}
	if cfg.TailscaleBin == "" {
		cfg.TailscaleBin = "tailscale"
	}
	if logger == nil {
		logger = log.Default()
	}
	httpClient, err := meshAgentHTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	return &MeshAgent{
		cfg:      cfg,
		logger:   logger,
		client:   httpClient,
		executor: OSCommandExecutor{},
		now:      func() time.Time { return time.Now().UTC() },
	}, nil
}

func (a *MeshAgent) SetCommandExecutor(executor CommandExecutor) {
	if executor == nil {
		executor = OSCommandExecutor{}
	}
	a.executor = executor
}

func (a *MeshAgent) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()
	if err := a.SyncOnce(ctx); err != nil {
		a.logger.Printf("initial mesh recovery sync failed: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := a.SyncOnce(ctx); err != nil {
				a.logger.Printf("mesh recovery sync failed: %v", err)
			}
		}
	}
}

func (a *MeshAgent) SyncOnce(ctx context.Context) error {
	state, _ := a.loadState()
	req := HeartbeatRequest{Node: a.cfg.Node}
	var lastErr error
	successes := make([]heartbeatResult, 0, len(a.cfg.Endpoints))
	for _, endpoint := range a.cfg.Endpoints {
		heartbeat, err := a.heartbeat(ctx, endpoint, req)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", endpoint, err)
			a.logger.Printf("mesh recovery endpoint %s sync failed: %v", endpoint, err)
			continue
		}
		if err := VerifyPeerDirectory(heartbeat.Directory, a.cfg.SigningKey, a.cfg.SigningKeyID, a.now()); err != nil {
			lastErr = fmt.Errorf("verify peer directory from %s: %w", endpoint, err)
			a.logger.Printf("mesh recovery endpoint %s returned invalid peer directory: %v", endpoint, err)
			continue
		}
		if err := VerifyGenerationManifest(heartbeat.Generation, a.cfg.SigningKey, a.cfg.SigningKeyID, a.now()); err != nil {
			lastErr = fmt.Errorf("verify generation manifest from %s: %w", endpoint, err)
			a.logger.Printf("mesh recovery endpoint %s returned invalid generation manifest: %v", endpoint, err)
			continue
		}
		successes = append(successes, heartbeatResult{endpoint: endpoint, response: heartbeat})
	}
	if len(successes) > 0 {
		chosen := chooseHeartbeatResult(successes)
		a.logGenerationMismatches(successes, chosen)
		if err := a.persistBundles(chosen.response.Directory, chosen.response.Generation); err != nil {
			return err
		}
		state.LastSyncAt = a.now()
		state.LastEndpoint = chosen.endpoint
		if chosen.response.Generation.RejoinRequired && state.Generation != chosen.response.Generation.Generation {
			if err := a.rejoin(ctx, chosen.endpoint, chosen.response.Generation, &state); err != nil {
				return err
			}
		} else if !chosen.response.Generation.RejoinRequired {
			state.Generation = chosen.response.Generation.Generation
		}
		return a.saveState(state)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no mesh recovery endpoint succeeded")
	}
	return lastErr
}

func chooseHeartbeatResult(results []heartbeatResult) heartbeatResult {
	chosen := results[0]
	for _, result := range results[1:] {
		chosenNodeCount := len(chosen.response.Directory.Nodes)
		resultNodeCount := len(result.response.Directory.Nodes)
		if resultNodeCount > chosenNodeCount {
			chosen = result
			continue
		}
		if resultNodeCount == chosenNodeCount && result.response.Directory.GeneratedAt.After(chosen.response.Directory.GeneratedAt) {
			chosen = result
		}
	}
	return chosen
}

func (a *MeshAgent) logGenerationMismatches(results []heartbeatResult, chosen heartbeatResult) {
	chosenGeneration := chosen.response.Generation
	for _, result := range results {
		generation := result.response.Generation
		if generation.Generation == chosenGeneration.Generation &&
			generation.Mode == chosenGeneration.Mode &&
			generation.RejoinRequired == chosenGeneration.RejoinRequired {
			continue
		}
		a.logger.Printf(
			"mesh recovery endpoint %s generation mismatch: got generation=%s mode=%s rejoin_required=%t; using endpoint %s generation=%s mode=%s rejoin_required=%t",
			result.endpoint,
			generation.Generation,
			generation.Mode,
			generation.RejoinRequired,
			chosen.endpoint,
			chosenGeneration.Generation,
			chosenGeneration.Mode,
			chosenGeneration.RejoinRequired,
		)
	}
}

func (a *MeshAgent) heartbeat(ctx context.Context, endpoint string, req HeartbeatRequest) (HeartbeatResponse, error) {
	var resp HeartbeatResponse
	if err := a.postJSON(ctx, endpoint, "/mesh/heartbeat", req, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (a *MeshAgent) rejoin(ctx context.Context, endpoint string, manifest GenerationManifest, state *agentState) error {
	if !a.cfg.RejoinEnabled {
		a.logger.Printf("mesh generation %s requires rejoin, but FUGUE_MESH_AGENT_REJOIN_ENABLED=false", manifest.Generation)
		return nil
	}
	var resp RejoinResponse
	if err := a.postJSON(ctx, endpoint, "/mesh/rejoin", map[string]string{"node_id": a.cfg.Node.NodeID}, &resp); err != nil {
		return err
	}
	if err := VerifyGenerationManifest(resp.Generation, a.cfg.SigningKey, a.cfg.SigningKeyID, a.now()); err != nil {
		return fmt.Errorf("verify rejoin generation manifest: %w", err)
	}
	if resp.Generation.Generation != manifest.Generation {
		return fmt.Errorf("rejoin generation mismatch: expected %s got %s", manifest.Generation, resp.Generation.Generation)
	}
	authKey := strings.TrimSpace(resp.AuthKey)
	if authKey == "" {
		return fmt.Errorf("mesh rejoin auth key missing")
	}
	if err := a.runTailscaleRejoin(ctx, authKey, manifest.LoginServer); err != nil {
		return err
	}
	state.Generation = manifest.Generation
	state.LastRejoinAt = a.now()
	return nil
}

func (a *MeshAgent) runTailscaleRejoin(ctx context.Context, authKey, loginServer string) error {
	if loginServer = strings.TrimSpace(loginServer); loginServer == "" {
		loginServer = strings.TrimSpace(a.cfg.LoginServer)
	}
	if loginServer == "" {
		return fmt.Errorf("mesh login server is required for tailscale rejoin")
	}
	if err := a.executor.Run(ctx, a.cfg.TailscaleBin, "logout"); err != nil {
		a.logger.Printf("tailscale logout before mesh rejoin failed; continuing: %v", err)
	}
	args := a.cfg.TailscaleArgs
	if len(args) == 0 {
		hostname := strings.TrimSpace(a.cfg.Node.Hostname)
		if hostname == "" {
			hostname = a.cfg.Node.NodeID
		}
		args = []string{"up", "--login-server=" + loginServer, "--authkey=" + authKey, "--hostname=" + hostname, "--reset"}
	} else {
		args = expandTailscaleArgs(args, map[string]string{
			"auth_key":     authKey,
			"login_server": loginServer,
			"hostname":     firstNonEmpty(a.cfg.Node.Hostname, a.cfg.Node.NodeID),
			"node_id":      a.cfg.Node.NodeID,
		})
	}
	if err := a.executor.Run(ctx, a.cfg.TailscaleBin, args...); err != nil {
		return fmt.Errorf("tailscale mesh rejoin failed: %w", err)
	}
	return nil
}

func (a *MeshAgent) postJSON(ctx context.Context, endpoint, path string, body any, out any) error {
	url := strings.TrimRight(endpoint, "/") + path
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.Token)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (a *MeshAgent) persistBundles(directory PeerDirectory, generation GenerationManifest) error {
	if a.cfg.DirectoryPath != "" {
		if err := writeJSONFile(a.cfg.DirectoryPath, directory, 0o644); err != nil {
			return fmt.Errorf("write peer directory: %w", err)
		}
	}
	if a.cfg.GenerationPath != "" {
		if err := writeJSONFile(a.cfg.GenerationPath, generation, 0o644); err != nil {
			return fmt.Errorf("write generation manifest: %w", err)
		}
	}
	return nil
}

func (a *MeshAgent) loadState() (agentState, error) {
	var state agentState
	if a.cfg.StatePath == "" {
		return state, nil
	}
	if err := readJSONFile(a.cfg.StatePath, &state); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return state, nil
		}
		return state, err
	}
	return state, nil
}

func (a *MeshAgent) saveState(state agentState) error {
	if a.cfg.StatePath == "" {
		return nil
	}
	return writeJSONFile(a.cfg.StatePath, state, 0o600)
}

func expandTailscaleArgs(args []string, values map[string]string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		for key, value := range values {
			arg = strings.ReplaceAll(arg, "{{"+key+"}}", value)
		}
		out = append(out, arg)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func meshAgentHTTPClient(cfg MeshAgentConfig) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.TrimSpace(cfg.CACertFile) != "" || cfg.TLSInsecureSkipVerify {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if cfg.TLSInsecureSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		if strings.TrimSpace(cfg.CACertFile) != "" {
			certPool, err := x509.SystemCertPool()
			if err != nil || certPool == nil {
				certPool = x509.NewCertPool()
			}
			certPEM, err := os.ReadFile(cfg.CACertFile)
			if err != nil {
				return nil, fmt.Errorf("read mesh agent CA cert file: %w", err)
			}
			if !certPool.AppendCertsFromPEM(certPEM) {
				return nil, fmt.Errorf("mesh agent CA cert file did not contain a valid PEM certificate")
			}
			tlsConfig.RootCAs = certPool
		}
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{Timeout: cfg.HTTPTimeout, Transport: transport}, nil
}
