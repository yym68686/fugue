package livediagnostics

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func testProbeCatalog() Catalog {
	image := "registry.example/diagnostic-probes@sha256:" + strings.Repeat("a", 64)
	return Catalog{Protocol: CatalogProtocol, Generation: 1, RunnerImage: image, Policy: CatalogPolicy{Namespaces: []string{"system-test"}, Profiles: []string{"cluster-read", "host-read"}, ServiceAccount: "diagnostic-reader"}, Probes: []Probe{{ID: "resource-observer", Image: image, Profile: "cluster-read", TargetTypes: []TargetType{TargetNode, TargetPlatformComponent}, MaxDurationSeconds: 60, Parameters: map[string]Parameter{"resource": {Required: true, Enum: []string{"pods", "events"}}}, Config: json.RawMessage(`{"collectors":[]}`)}}}
}
func TestCatalogRejectsTamperingAndRevokedKeys(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	c := testProbeCatalog()
	signed, err := SignCatalog(c, "publisher", key)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{"publisher": base64.StdEncoding.EncodeToString(pub)}
	cache := &CatalogCache{}
	verified, err := cache.Load(signed, keys)
	if err != nil {
		t.Fatal(err)
	}
	var tampered SignedCatalog
	if err := json.Unmarshal(signed, &tampered); err != nil {
		t.Fatal(err)
	}
	changed := c
	changed.Probes = append([]Probe(nil), c.Probes...)
	changed.Probes[0].Image = "registry.example/other@sha256:" + strings.Repeat("b", 64)
	payload, _ := json.Marshal(changed)
	tampered.Payload = base64.StdEncoding.EncodeToString(payload)
	bad, _ := json.Marshal(tampered)
	if _, err := VerifyCatalog(bad, keys); err == nil {
		t.Fatal("accepted manifest image tampering")
	}
	retained, err := cache.Load(bad, keys)
	if err != nil || retained.Digest != verified.Digest || retained.Status != "last_known_good" {
		t.Fatalf("failed to preserve verified catalog: %+v %v", retained, err)
	}
	if _, err := cache.Load(bad, map[string]string{}); err == nil {
		t.Fatal("revoked key still authorized a cached probe")
	}
}
func TestProbeReferencesAndParametersArePinned(t *testing.T) {
	c := VerifiedCatalog{Catalog: testProbeCatalog(), Digest: "catalog"}
	probe := c.Probes[0]
	if _, _, err := c.Resolve(probe.ID+"@sha256:"+strings.Repeat("f", 64), map[string]string{"resource": "pods"}); err == nil {
		t.Fatal("accepted different manifest digest")
	}
	for _, params := range []map[string]string{{}, {"resource": "secrets"}, {"resource": "pods", "image": "untrusted"}} {
		if _, _, err := c.Resolve(probe.ID, params); err == nil {
			t.Fatalf("accepted unauthorized parameters: %v", params)
		}
	}
	if _, _, err := c.Resolve(probe.ID+"@"+probe.Digest(), map[string]string{"resource": "pods"}); err != nil {
		t.Fatal(err)
	}
}
func TestProbeCapabilityProfilesCannotEscalateViaParameters(t *testing.T) {
	c := VerifiedCatalog{Catalog: testProbeCatalog(), Digest: "catalog"}
	target := Target{Type: TargetNode, Node: "node-test"}
	job, err := BuildProbeJob(c, c.Probes[0], target, "diagnostic-test", "system-test", "api", StartRequest{DurationSeconds: 30}, map[string]string{"resource": "pods"})
	if err != nil {
		t.Fatal(err)
	}
	pod := job.Spec.Template.Spec
	if pod.HostPID || len(pod.Containers[0].SecurityContext.Capabilities.Add) > 0 || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatalf("read-only cluster probe acquired host privileges: %+v", pod)
	}
	if pod.ServiceAccountName != "diagnostic-reader" || pod.Containers[0].Image != c.Probes[0].Image {
		t.Fatal("probe did not use approved identity and image")
	}
	target = Target{Type: TargetPlatformComponent, Namespace: "other-tenant", Pod: "app-test", PodUID: "uid-test", Container: "app", ContainerID: "containerd://0123456789abcdef", Node: "node-test"}
	if _, err := BuildProbeJob(c, c.Probes[0], target, "diagnostic-test", "system-test", "api", StartRequest{DurationSeconds: 30}, map[string]string{"resource": "pods"}); err == nil {
		t.Fatal("target namespace escaped catalog authorization")
	}
	target = Target{Type: TargetNode, Node: "node-test"}
	if _, err := BuildProbeJob(c, c.Probes[0], target, "diagnostic-test", "system-test", "api", StartRequest{DurationSeconds: 61}, map[string]string{"resource": "pods"}); err == nil {
		t.Fatal("accepted duration above probe budget")
	}
}

func TestPublishedCatalogKeepsTrustedPreviousAfterColdRestart(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	signed, err := SignCatalog(testProbeCatalog(), "publisher", key)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{"publisher": base64.StdEncoding.EncodeToString(pub)}
	recovered, err := LoadPublishedCatalog([]byte(`{"signature":"invalid"}`), signed, keys, &CatalogCache{})
	if err != nil || recovered.Status != "last_known_good" {
		t.Fatalf("cold restart lost trusted catalog: %+v %v", recovered, err)
	}
	if _, err := LoadPublishedCatalog([]byte(`{}`), signed, map[string]string{}, &CatalogCache{}); err == nil {
		t.Fatal("revoked persisted catalog was accepted")
	}
}
