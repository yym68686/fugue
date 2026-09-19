package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImmutableRegistryVerificationIsScopedToOneExecution(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "verify.py")
	marker := filepath.Join(root, "calls")
	image := "ghcr.io/example/edge@sha256:" + strings.Repeat("b", 64)
	revision := strings.Repeat("a", 40)
	program := `import json,os,sys
with open(os.environ['REGISTRY_TEST_CALLS'],'a') as f: f.write('x')
image=sys.argv[sys.argv.index('--image')+1]
revision=sys.argv[sys.argv.index('--expected-revision')+1]
if revision != 'a'*40: raise SystemExit(19)
print(json.dumps({'image':image,'index_digest':'sha256:'+'b'*64,'manifest_digest':'sha256:'+'c'*64,'config_digest':'sha256:'+'d'*64,'oci_revision':revision,'platform':'linux/amd64','verification':'registry_manifest_config_get','blob_count':0,'layer_get_probe_count':0,'request_count':3,'total_layer_bytes':0}))
`
	if err := os.WriteFile(script, []byte(program), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REGISTRY_TEST_CALLS", marker)
	count := func() int { raw, _ := os.ReadFile(marker); return len(raw) }
	cluster := &kubectlCluster{verifier: script}
	first, err := cluster.verifyRuntimeArtifact(context.Background(), image, revision)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cluster.verifyRuntimeArtifact(context.Background(), image, revision)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if err != nil || count() != 1 || string(a) != string(b) {
		t.Fatal("immutable successful evidence was fetched again", err, count())
	}
	for i := 0; i < 2; i++ {
		if _, err = cluster.verifyRuntimeArtifact(context.Background(), image, strings.Repeat("e", 40)); err == nil {
			t.Fatal("different revision inherited cached trust")
		}
	}
	if count() != 3 {
		t.Fatal("failed verification was cached")
	}
	if _, err = cluster.verifyRuntimeArtifact(context.Background(), strings.Replace(image, "example/edge", "example/other", 1), revision); err != nil || count() != 4 {
		t.Fatal("different repository inherited trust", err)
	}
	fresh := &kubectlCluster{verifier: script}
	if _, err = fresh.verifyRuntimeArtifact(context.Background(), image, revision); err != nil || count() != 5 {
		t.Fatal("new execution reused old cache", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = cluster.verifyRuntimeArtifact(ctx, image, revision); err == nil || count() != 5 {
		t.Fatal("cancelled execution used cached trust")
	}
}
