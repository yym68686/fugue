package declarativerelease

import (
	"bytes"
	"strings"
	"testing"
)

func TestOwnershipAdoptionManifestContainsOnlyReviewedLKGFieldsAndCAS(t *testing.T) {
	lkg := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"labels":{"owner":"legacy"},"name":"edge-alpha-front","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"front"}},"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"1111111111111111111111111111111111111111"}},"spec":{"containers":[{"image":"example/edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"edge-front"}]}}}}],"kind":"ComponentResourceSet"}`)
	plan := OwnershipAdoptionPlan{
		BootstrapLKGDigest: digestOf(lkg),
		Resources: []OwnershipAdoptionResourcePlan{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-alpha-front"},
			Fields:   []string{"/spec/template"}, UID: "front-uid", ResourceVersion: "42", Generation: 7,
		}},
	}
	raw, err := BuildOwnershipAdoptionManifest(lkg, plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"uid":"front-uid"`, `"resourceVersion":"42"`, `"spec":{"template"`, `"fugue.pro/source-commit"`} {
		if !bytes.Contains(raw, []byte(required)) {
			t.Fatalf("adoption manifest lost %s: %s", required, raw)
		}
	}
	for _, forbidden := range []string{`"selector"`, `"owner":"legacy"`, `"generation"`} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatalf("adoption manifest claimed unreviewed field %s: %s", forbidden, raw)
		}
	}
	plan.Resources[0].Fields = []string{"/spec/missing"}
	if _, err := BuildOwnershipAdoptionManifest(lkg, plan); err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("absent adoption field was accepted: %v", err)
	}
}
