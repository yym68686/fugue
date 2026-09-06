package sshfront

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreferNodeScopedEdgeToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge-node.env")
	if err := os.WriteFile(path, []byte("FUGUE_EDGE_NODE_TOKEN=node-scoped\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := preferNodeScopedEdgeToken(path, "static"); got != "node-scoped" {
		t.Fatalf("got %q", got)
	}
	if got := preferNodeScopedEdgeToken(filepath.Join(t.TempDir(), "missing"), "static"); got != "static" {
		t.Fatalf("got %q", got)
	}
}

func TestPreferNodeScopedEdgeTokenAcceptsLegacyAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge-node.env")
	if err := os.WriteFile(path, []byte("FUGUE_EDGE_TOKEN=legacy-node\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := preferNodeScopedEdgeToken(path, "static"); got != "legacy-node" {
		t.Fatalf("got %q", got)
	}
}
