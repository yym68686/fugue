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

func TestOwnershipTakeoverManifestContainsOnlyReviewedImmutableTargetFields(t *testing.T) {
	target := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"labels":{"owner":"declarative"},"name":"edge-alpha-front","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"front"}},"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"2222222222222222222222222222222222222222"}},"spec":{"containers":[{"image":"example/edge@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"edge-front"}]}}}}],"kind":"ComponentResourceSet"}`)
	plan := OwnershipAdoptionPlan{
		BootstrapLKGDigest: "sha256:" + strings.Repeat("a", 64), AlreadyConverged: true,
		Resources: []OwnershipAdoptionResourcePlan{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-alpha-front"},
			Fields:   []string{"/spec/template"}, UID: "front-uid", ResourceVersion: "42", Generation: 7,
		}},
	}
	identity := TargetIdentity{Present: true, ImageRef: "example/edge@sha256:" + strings.Repeat("b", 64),
		ConfigSHA: strings.Repeat("2", 40), ManifestSHA: strings.Repeat("2", 40), OCIRevision: strings.Repeat("2", 40), ManifestDigest: digestOf(target)}
	raw, err := BuildOwnershipTakeoverManifest(target, plan, identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"uid":"front-uid"`, `"resourceVersion":"42"`, strings.Repeat("2", 40), strings.Repeat("b", 64)} {
		if !bytes.Contains(raw, []byte(required)) {
			t.Fatalf("ownership takeover lost %s: %s", required, raw)
		}
	}
	if bytes.Contains(raw, []byte(`"selector"`)) || bytes.Contains(raw, []byte(`"owner":"declarative"`)) {
		t.Fatalf("ownership takeover claimed unreviewed fields: %s", raw)
	}
	identity.ManifestDigest = "sha256:" + strings.Repeat("c", 64)
	if _, err := BuildOwnershipTakeoverManifest(target, plan, identity); err == nil {
		t.Fatal("ownership takeover accepted a mismatched immutable target")
	}
}
