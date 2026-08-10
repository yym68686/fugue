package releaseguardian

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestCanaryResultIsImmutableRecordBoundAndFresh(t *testing.T) {
	key := Key{Component: "edge-control-de", Group: "de"}
	record, err := NewReleaseRecord(key, testSHA, testDigest, testDigest, otherDigest, testDigest)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	result, err := NewCanaryResult(record, HealthHealthy, testDigest, now, now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(now.Add(29 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(now.Add(31 * time.Second)); err == nil {
		t.Fatal("expired canary result was accepted")
	}
	result.RecordDigest = otherDigest
	if err := result.Validate(now); err == nil {
		t.Fatal("record drift did not invalidate canary result digest")
	}
}

func TestObjectNamesRemainComponentAndGroupScoped(t *testing.T) {
	de := Key{Component: "edge-control", Group: "edge-pool-a"}
	us := Key{Component: "edge-control", Group: "edge-pool-b"}
	if statusName(de) == statusName(us) || canaryName(de) == canaryName(us) {
		t.Fatal("group-scoped objects collided")
	}
}

func TestGuardianWriterResourcesKeepIndependentProberAndComponentScopedRBAC(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/guardian/resources.json")
	if err != nil {
		t.Fatal(err)
	}
	set, err := declarativerelease.DecodeResourceSet(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Items) != 8 {
		t.Fatalf("resource count=%d", len(set.Items))
	}
	encoded, _ := json.Marshal(set)
	source := string(encoded)
	for _, required := range []string{
		`"name":"fugue-release-guardian"`, `"name":"fugue-release-canary-prober"`,
		`"value":"write"`, `"value":"guardian"`, `"value":"canary-prober"`,
		`"fieldPath":"metadata.uid"`, `"mountPath":"/tmp"`,
		`"resources":["deployments"],"verbs":["create","delete","get","list","patch","update","watch"]`,
		`"resources":["daemonsets"],"verbs":["get","list","watch"]`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("Guardian resources lack %s", required)
		}
	}
	for _, forbidden := range []string{`"secrets"`, `"daemonsets/status"`, `"deployments/status"`, `"clusterroles"`, `"clusterrolebindings"`} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("shadow Guardian resources grant forbidden capability %s", forbidden)
		}
	}
}
