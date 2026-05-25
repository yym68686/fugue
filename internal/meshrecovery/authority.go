package meshrecovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type RecoveryAuthority struct {
	cfg    RecoveryConfig
	logger *log.Logger
	now    func() time.Time

	mu    sync.RWMutex
	nodes map[string]MeshNode
}

type recoveryState struct {
	Nodes []MeshNode `json:"nodes"`
}

func NewRecoveryAuthority(cfg RecoveryConfig, logger *log.Logger) (*RecoveryAuthority, error) {
	cfg.SigningKey = strings.TrimSpace(cfg.SigningKey)
	if cfg.SigningKey == "" {
		return nil, ErrMissingSigningKey
	}
	cfg.SigningKeyID = strings.TrimSpace(cfg.SigningKeyID)
	if cfg.SigningKeyID == "" {
		cfg.SigningKeyID = "mesh-recovery"
	}
	cfg.ListenAddr = strings.TrimSpace(cfg.ListenAddr)
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:7840"
	}
	cfg.Generation = strings.TrimSpace(cfg.Generation)
	if cfg.Generation == "" {
		cfg.Generation = "meshgen-initial"
	}
	cfg.Mode = normalizeMode(cfg.Mode)
	cfg.Issuer = strings.TrimSpace(cfg.Issuer)
	if cfg.Issuer == "" {
		cfg.Issuer = DefaultIssuer
	}
	if cfg.DirectoryValidFor <= 0 {
		cfg.DirectoryValidFor = 2 * time.Minute
	}
	if cfg.ManifestValidFor <= 0 {
		cfg.ManifestValidFor = 2 * time.Minute
	}
	if cfg.NodeTTL <= 0 {
		cfg.NodeTTL = 2 * time.Minute
	}
	if logger == nil {
		logger = log.Default()
	}
	authority := &RecoveryAuthority{
		cfg:    cfg,
		logger: logger,
		now:    func() time.Time { return time.Now().UTC() },
		nodes:  make(map[string]MeshNode),
	}
	if err := authority.load(); err != nil {
		return nil, err
	}
	return authority, nil
}

func (a *RecoveryAuthority) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              a.cfg.ListenAddr,
		Handler:           a.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		a.logger.Printf("mesh recovery authority listening on %s", a.cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (a *RecoveryAuthority) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/mesh/directory", a.handleDirectory)
	mux.HandleFunc("/mesh/generation", a.handleGeneration)
	mux.HandleFunc("/mesh/heartbeat", a.handleHeartbeat)
	mux.HandleFunc("/mesh/rejoin", a.handleRejoin)
	return mux
}

func (a *RecoveryAuthority) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"generation": a.cfg.Generation,
		"mode":       a.cfg.Mode,
	})
}

func (a *RecoveryAuthority) handleDirectory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	directory, err := a.Directory()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, directory)
}

func (a *RecoveryAuthority) handleGeneration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manifest, err := a.GenerationManifest()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

func (a *RecoveryAuthority) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !a.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	defer r.Body.Close()
	var req HeartbeatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid heartbeat body", http.StatusBadRequest)
		return
	}
	node, err := a.updateNode(req.Node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.save(); err != nil {
		a.logger.Printf("failed to save mesh recovery state after heartbeat from %s: %v", node.NodeID, err)
	}
	directory, err := a.Directory()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	manifest, err := a.GenerationManifest()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, HeartbeatResponse{Directory: directory, Generation: manifest})
}

func (a *RecoveryAuthority) handleRejoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !a.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	manifest, err := a.GenerationManifest()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := RejoinResponse{Generation: manifest}
	if manifest.RejoinRequired {
		resp.AuthKey = a.cfg.RejoinAuthKey
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *RecoveryAuthority) Directory() (PeerDirectory, error) {
	now := a.now()
	a.mu.RLock()
	nodes := make([]MeshNode, 0, len(a.nodes))
	for _, node := range a.nodes {
		node = markNodeStatus(node, now, a.cfg.NodeTTL)
		nodes = append(nodes, node)
	}
	a.mu.RUnlock()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	directory := PeerDirectory{
		SchemaVersion: SchemaVersionV1,
		Generation:    a.cfg.Generation,
		GeneratedAt:   now,
		Issuer:        a.cfg.Issuer,
		LoginServer:   a.cfg.LoginServer,
		Nodes:         nodes,
	}
	return SignPeerDirectory(directory, a.cfg.SigningKey, a.cfg.SigningKeyID, a.cfg.DirectoryValidFor, now)
}

func (a *RecoveryAuthority) GenerationManifest() (GenerationManifest, error) {
	now := a.now()
	manifest := GenerationManifest{
		SchemaVersion:      SchemaVersionV1,
		Generation:         a.cfg.Generation,
		PreviousGeneration: a.cfg.PreviousGeneration,
		Mode:               a.cfg.Mode,
		LoginServer:        a.cfg.LoginServer,
		RejoinRequired:     a.cfg.Mode == GenerationModeReset,
		Message:            a.cfg.Message,
		IssuedAt:           now,
		Issuer:             a.cfg.Issuer,
	}
	return SignGenerationManifest(manifest, a.cfg.SigningKey, a.cfg.SigningKeyID, a.cfg.ManifestValidFor, now)
}

func (a *RecoveryAuthority) updateNode(node MeshNode) (MeshNode, error) {
	node.NodeID = strings.TrimSpace(node.NodeID)
	if node.NodeID == "" {
		return node, fmt.Errorf("node_id is required")
	}
	node.Hostname = strings.TrimSpace(node.Hostname)
	node.PublicIPv4 = strings.TrimSpace(node.PublicIPv4)
	node.PublicIPv6 = strings.TrimSpace(node.PublicIPv6)
	node.PrivateIPv4 = strings.TrimSpace(node.PrivateIPv4)
	node.MeshIP = strings.TrimSpace(node.MeshIP)
	node.Roles = cleanList(node.Roles)
	node.APIEndpoints = cleanList(node.APIEndpoints)
	node.RecoveryEndpoints = cleanList(node.RecoveryEndpoints)
	node.EdgeEndpoints = cleanList(node.EdgeEndpoints)
	node.LastSeen = a.now()
	node.TTLSeconds = int(a.cfg.NodeTTL.Seconds())
	node.Status = NodeStatusHealthy
	node.Source = "heartbeat"
	a.mu.Lock()
	a.nodes[node.NodeID] = node
	a.mu.Unlock()
	return node, nil
}

func (a *RecoveryAuthority) authorized(r *http.Request) bool {
	token := strings.TrimSpace(a.cfg.Token)
	if token == "" {
		return true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") && strings.TrimSpace(auth[7:]) == token {
		return true
	}
	return strings.TrimSpace(r.Header.Get("X-Fugue-Mesh-Recovery-Token")) == token
}

func (a *RecoveryAuthority) load() error {
	if a.cfg.SeedPath != "" {
		var seed recoveryState
		if err := readJSONFile(a.cfg.SeedPath, &seed); err != nil {
			return fmt.Errorf("load mesh recovery seed: %w", err)
		}
		for _, node := range seed.Nodes {
			node.NodeID = strings.TrimSpace(node.NodeID)
			if node.NodeID == "" {
				continue
			}
			if node.LastSeen.IsZero() {
				node.LastSeen = a.now()
			}
			if node.TTLSeconds <= 0 {
				node.TTLSeconds = int(a.cfg.NodeTTL.Seconds())
			}
			node.Source = "seed"
			a.nodes[node.NodeID] = node
		}
	}
	if a.cfg.StatePath == "" {
		return nil
	}
	var state recoveryState
	if err := readJSONFile(a.cfg.StatePath, &state); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("load mesh recovery state: %w", err)
	}
	for _, node := range state.Nodes {
		node.NodeID = strings.TrimSpace(node.NodeID)
		if node.NodeID == "" {
			continue
		}
		a.nodes[node.NodeID] = node
	}
	return nil
}

func (a *RecoveryAuthority) save() error {
	if a.cfg.StatePath == "" {
		return nil
	}
	a.mu.RLock()
	nodes := make([]MeshNode, 0, len(a.nodes))
	for _, node := range a.nodes {
		nodes = append(nodes, node)
	}
	a.mu.RUnlock()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	return writeJSONFile(a.cfg.StatePath, recoveryState{Nodes: nodes}, 0o600)
}

func markNodeStatus(node MeshNode, now time.Time, fallbackTTL time.Duration) MeshNode {
	ttl := time.Duration(node.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = fallbackTTL
	}
	if ttl > 0 && !node.LastSeen.IsZero() && now.After(node.LastSeen.Add(ttl)) {
		node.Status = NodeStatusStale
		return node
	}
	if strings.TrimSpace(node.Status) == "" {
		node.Status = NodeStatusHealthy
	}
	return node
}

func normalizeMode(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case GenerationModeReset:
		return GenerationModeReset
	default:
		return GenerationModeNormal
	}
}

func cleanList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
