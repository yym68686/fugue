package sshfront

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestRouteURLNeverCarriesCredentials(t *testing.T) {
	address, err := sshRoutesURL(Config{APIURL: "https://control.example?token=old-secret&region=sample", EdgeToken: "current-secret", EdgeID: "edge", EdgeGroupID: "group"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Query().Has("token") || parsed.Query().Get("edge_id") != "edge" || parsed.Query().Get("region") != "sample" {
		t.Fatal("route URL leaked a credential or lost its declared selector")
	}
}

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
