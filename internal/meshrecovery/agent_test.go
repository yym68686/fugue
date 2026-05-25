package meshrecovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

type recordingExecutor struct {
	calls []execCall
}

type execCall struct {
	name string
	args []string
}

func (e *recordingExecutor) Run(_ context.Context, name string, args ...string) error {
	copied := append([]string(nil), args...)
	e.calls = append(e.calls, execCall{name: name, args: copied})
	return nil
}

func TestMeshAgentSyncPersistsSignedBundles(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	authority, err := NewRecoveryAuthority(RecoveryConfig{
		StatePath:         filepath.Join(t.TempDir(), "authority-state.json"),
		Generation:        "meshgen-1",
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
	server := httptest.NewServer(authority.Handler())
	defer server.Close()

	dir := t.TempDir()
	agent, err := NewMeshAgent(MeshAgentConfig{
		Endpoints:      []string{server.URL},
		Token:          "token",
		SigningKey:     "signing-key",
		SigningKeyID:   "key-1",
		StatePath:      filepath.Join(dir, "state.json"),
		DirectoryPath:  filepath.Join(dir, "directory.json"),
		GenerationPath: filepath.Join(dir, "generation.json"),
		Node: MeshNode{
			NodeID:   "node-a",
			Hostname: "node-a",
			Roles:    []string{"agent"},
			MeshIP:   "100.64.0.10",
		},
	}, nil)
	if err != nil {
		t.Fatalf("new mesh agent: %v", err)
	}
	agent.now = func() time.Time { return now }

	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	var directory PeerDirectory
	if err := readJSONFile(filepath.Join(dir, "directory.json"), &directory); err != nil {
		t.Fatalf("read directory: %v", err)
	}
	if err := VerifyPeerDirectory(directory, "signing-key", "key-1", now); err != nil {
		t.Fatalf("verify persisted directory: %v", err)
	}
	var state agentState
	if err := readJSONFile(filepath.Join(dir, "state.json"), &state); err != nil {
		t.Fatalf("read agent state: %v", err)
	}
	if state.Generation != "meshgen-1" || state.LastEndpoint != server.URL {
		t.Fatalf("unexpected agent state: %#v", state)
	}
}

func TestMeshAgentHeartbeatsAllRecoveryEndpoints(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	newAuthority := func(t *testing.T) (*RecoveryAuthority, *httptest.Server) {
		t.Helper()
		authority, err := NewRecoveryAuthority(RecoveryConfig{
			StatePath:         filepath.Join(t.TempDir(), "authority-state.json"),
			Generation:        "meshgen-1",
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
		return authority, httptest.NewServer(authority.Handler())
	}
	authorityA, serverA := newAuthority(t)
	defer serverA.Close()
	authorityB, serverB := newAuthority(t)
	defer serverB.Close()

	agent, err := NewMeshAgent(MeshAgentConfig{
		Endpoints:    []string{serverA.URL, serverB.URL},
		Token:        "token",
		SigningKey:   "signing-key",
		SigningKeyID: "key-1",
		StatePath:    filepath.Join(t.TempDir(), "state.json"),
		Node: MeshNode{
			NodeID:   "node-a",
			Hostname: "node-a",
			Roles:    []string{"agent"},
			MeshIP:   "100.64.0.10",
		},
	}, nil)
	if err != nil {
		t.Fatalf("new mesh agent: %v", err)
	}
	agent.now = func() time.Time { return now }

	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	for name, authority := range map[string]*RecoveryAuthority{"a": authorityA, "b": authorityB} {
		directory, err := authority.Directory()
		if err != nil {
			t.Fatalf("directory %s: %v", name, err)
		}
		if len(directory.Nodes) != 1 || directory.Nodes[0].NodeID != "node-a" {
			t.Fatalf("authority %s did not receive heartbeat: %#v", name, directory.Nodes)
		}
	}
}

func TestMeshAgentResetGenerationRunsTailscaleRejoin(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	mux := http.NewServeMux()
	mux.HandleFunc("/mesh/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		manifest, err := SignGenerationManifest(GenerationManifest{
			Generation:     "meshgen-reset",
			Mode:           GenerationModeReset,
			LoginServer:    "https://mesh.example.test",
			RejoinRequired: true,
			IssuedAt:       now,
		}, "signing-key", "key-1", time.Minute, now)
		if err != nil {
			t.Fatalf("sign manifest: %v", err)
		}
		directory, err := SignPeerDirectory(PeerDirectory{
			Generation:  "meshgen-reset",
			GeneratedAt: now,
			LoginServer: "https://mesh.example.test",
			Nodes:       []MeshNode{{NodeID: "node-a", Status: NodeStatusHealthy}},
		}, "signing-key", "key-1", time.Minute, now)
		if err != nil {
			t.Fatalf("sign directory: %v", err)
		}
		_ = json.NewEncoder(w).Encode(HeartbeatResponse{Directory: directory, Generation: manifest})
	})
	mux.HandleFunc("/mesh/rejoin", func(w http.ResponseWriter, r *http.Request) {
		manifest, err := SignGenerationManifest(GenerationManifest{
			Generation:     "meshgen-reset",
			Mode:           GenerationModeReset,
			LoginServer:    "https://mesh.example.test",
			RejoinRequired: true,
			IssuedAt:       now,
		}, "signing-key", "key-1", time.Minute, now)
		if err != nil {
			t.Fatalf("sign manifest: %v", err)
		}
		_ = json.NewEncoder(w).Encode(RejoinResponse{Generation: manifest, AuthKey: "tskey-auth-secret"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := t.TempDir()
	agent, err := NewMeshAgent(MeshAgentConfig{
		Endpoints:      []string{server.URL},
		Token:          "token",
		SigningKey:     "signing-key",
		SigningKeyID:   "key-1",
		StatePath:      filepath.Join(dir, "state.json"),
		DirectoryPath:  filepath.Join(dir, "directory.json"),
		GenerationPath: filepath.Join(dir, "generation.json"),
		RejoinEnabled:  true,
		TailscaleBin:   "tailscale",
		Node: MeshNode{
			NodeID:   "node-a",
			Hostname: "node-a",
		},
	}, nil)
	if err != nil {
		t.Fatalf("new mesh agent: %v", err)
	}
	agent.now = func() time.Time { return now }
	executor := &recordingExecutor{}
	agent.SetCommandExecutor(executor)

	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	if len(executor.calls) != 2 {
		t.Fatalf("expected logout and up calls, got %#v", executor.calls)
	}
	if executor.calls[0].name != "tailscale" || len(executor.calls[0].args) != 1 || executor.calls[0].args[0] != "logout" {
		t.Fatalf("unexpected logout call: %#v", executor.calls[0])
	}
	up := executor.calls[1]
	if up.name != "tailscale" {
		t.Fatalf("unexpected up command: %#v", up)
	}
	wantArgs := []string{"up", "--login-server=https://mesh.example.test", "--authkey=tskey-auth-secret", "--hostname=node-a", "--reset"}
	if len(up.args) != len(wantArgs) {
		t.Fatalf("unexpected up args: %#v", up.args)
	}
	for i, want := range wantArgs {
		if up.args[i] != want {
			t.Fatalf("up arg %d: got %q want %q", i, up.args[i], want)
		}
	}
	var state agentState
	if err := readJSONFile(filepath.Join(dir, "state.json"), &state); err != nil {
		t.Fatalf("read agent state: %v", err)
	}
	if state.Generation != "meshgen-reset" || state.LastRejoinAt.IsZero() {
		t.Fatalf("unexpected agent state after rejoin: %#v", state)
	}
}
