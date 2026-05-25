package meshrecovery

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryAuthorityHeartbeatBuildsSignedDirectory(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	authority, err := NewRecoveryAuthority(RecoveryConfig{
		StatePath:         filepath.Join(t.TempDir(), "state.json"),
		Generation:        "meshgen-1",
		Mode:              GenerationModeNormal,
		LoginServer:       "https://mesh.example.test",
		SigningKey:        "signing-key",
		SigningKeyID:      "key-1",
		Token:             "token",
		DirectoryValidFor: time.Minute,
		ManifestValidFor:  time.Minute,
		NodeTTL:           time.Minute,
	}, nil)
	if err != nil {
		t.Fatalf("new recovery authority: %v", err)
	}
	authority.now = func() time.Time { return now }

	body, _ := json.Marshal(HeartbeatRequest{Node: MeshNode{
		NodeID:       "node-a",
		Hostname:     "node-a",
		Roles:        []string{"edge", "dns", "edge"},
		MeshIP:       "100.64.0.10",
		PublicIPv4:   "203.0.113.10",
		APIEndpoints: []string{"https://api.example.test"},
	}})
	req := httptest.NewRequest(http.MethodPost, "/mesh/heartbeat", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	authority.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("heartbeat returned %d: %s", rec.Code, rec.Body.String())
	}

	var resp HeartbeatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode heartbeat response: %v", err)
	}
	if err := VerifyPeerDirectory(resp.Directory, "signing-key", "key-1", now); err != nil {
		t.Fatalf("verify directory: %v", err)
	}
	if err := VerifyGenerationManifest(resp.Generation, "signing-key", "key-1", now); err != nil {
		t.Fatalf("verify generation: %v", err)
	}
	if len(resp.Directory.Nodes) != 1 {
		t.Fatalf("expected one node, got %#v", resp.Directory.Nodes)
	}
	node := resp.Directory.Nodes[0]
	if node.NodeID != "node-a" || node.Status != NodeStatusHealthy || len(node.Roles) != 2 {
		t.Fatalf("unexpected node entry: %#v", node)
	}
}

func TestRecoveryAuthorityRequiresTokenWhenConfigured(t *testing.T) {
	authority, err := NewRecoveryAuthority(RecoveryConfig{
		SigningKey:   "signing-key",
		SigningKeyID: "key-1",
		Token:        "token",
	}, nil)
	if err != nil {
		t.Fatalf("new recovery authority: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mesh/heartbeat", bytes.NewReader([]byte(`{"node":{"node_id":"node-a"}}`)))
	rec := httptest.NewRecorder()
	authority.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", rec.Code)
	}
}

func TestRecoveryAuthorityResetRejoinReturnsAuthKey(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	authority, err := NewRecoveryAuthority(RecoveryConfig{
		Generation:       "meshgen-reset",
		Mode:             GenerationModeReset,
		LoginServer:      "https://mesh.example.test",
		SigningKey:       "signing-key",
		SigningKeyID:     "key-1",
		Token:            "token",
		RejoinAuthKey:    "tskey-auth-redacted",
		ManifestValidFor: time.Minute,
	}, nil)
	if err != nil {
		t.Fatalf("new recovery authority: %v", err)
	}
	authority.now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "/mesh/rejoin", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	authority.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rejoin returned %d: %s", rec.Code, rec.Body.String())
	}
	var resp RejoinResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode rejoin response: %v", err)
	}
	if resp.AuthKey != "tskey-auth-redacted" {
		t.Fatalf("expected rejoin auth key")
	}
	if !resp.Generation.RejoinRequired {
		t.Fatalf("expected rejoin required")
	}
}
