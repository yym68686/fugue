package releaseguardian

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
)

func TestIndependentRegistryRecoveryMirror(t *testing.T) {
	for _, mode := range []string{"verified", "wrong receipt pin", "tampered receipt", "wrong host", "missing pin", "tag moved", "missing rollback layer"} {
		t.Run(mode, func(t *testing.T) {
			objects := map[string]registryTestContent{}
			forward, forwardLayer := registryImageFixture(t, objects, "fixture/recovery", strings.Repeat("a", 40), true)
			rollback, rollbackLayer := registryImageFixture(t, objects, "fixture/recovery", strings.Repeat("b", 40), false)
			repo := forward.Repository
			pinPath := func(d string) string {
				return "/v2/fixture/recovery/manifests/fugue-recovery-sha256-" + strings.TrimPrefix(d, "sha256:")
			}
			forwardPin, rollbackPin := pinPath(forward.TopDigest), pinPath(rollback.TopDigest)
			forwardTop := objects["/v2/fixture/recovery/manifests/"+forward.TopDigest]
			rollbackTop := objects["/v2/fixture/recovery/manifests/"+rollback.TopDigest]
			objects[forwardPin], objects[rollbackPin] = forwardTop, rollbackTop
			if mode == "missing pin" {
				delete(objects, forwardPin)
			}
			if mode == "missing rollback layer" {
				delete(objects, rollbackLayer)
			}
			var seen = map[string]int{}
			registry, err := NewIndependentRegistry(testNativeRegistry, nil, registryRoundTrip(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host != testNativeRegistry || req.URL.Scheme != "https" || req.Method != http.MethodGet {
					t.Errorf("unexpected registry request: %s %s", req.Method, req.URL)
				}
				seen[req.URL.Path]++
				header := http.Header{}
				status := http.StatusOK
				obj, ok := objects[req.URL.Path]
				if !ok && req.URL.Path != "/v2/" {
					status = http.StatusNotFound
				}
				if mode == "tag moved" && req.URL.Path == rollbackPin && seen[rollbackPin] == 2 {
					obj = forwardTop
				}
				header.Set("Content-Type", string(obj.mediaType))
				header.Set("Docker-Distribution-API-Version", "registry/2.0")
				return &http.Response{StatusCode: status, Header: header, ContentLength: int64(len(obj.body)),
					Body: io.NopCloser(strings.NewReader(string(obj.body))), Request: req}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			sealed := func(fixture declarativerelease.ArtifactReceipt, sourceRepo string) RegistryRecoveryArtifact {
				t.Helper()
				fixture.APIVersion, fixture.Kind = declarativerelease.ArtifactReceiptAPIVersion, declarativerelease.ArtifactReceiptKind
				fixture.Component, fixture.ConfigSHA = "fixture-app", fixture.OCIRevision
				fixture.SourceSHA, fixture.SourceTag = fixture.OCIRevision, fixture.OCIRevision
				fixture.PlanDigest, fixture.IntentDigest = testDigest, testDigest
				fixture.Repository, fixture.ImmutableRef = sourceRepo, sourceRepo+"@"+fixture.TopDigest
				fixture.Verification = "registry_manifest_config_and_layer_get"
				fixture.ReceiptDigest = ""
				raw, err := declarativerelease.CanonicalJSON(fixture)
				if err != nil {
					t.Fatal(err)
				}
				fixture.ReceiptDigest = digest(raw)
				return RegistryRecoveryArtifact{fixture, fixture.ReceiptDigest, repo}
			}
			a, b := sealed(forward, "ghcr.io/fixture/app"), sealed(rollback, "ghcr.io/fixture/app")
			switch mode {
			case "wrong receipt pin":
				a.SourceReceiptDigest = b.SourceReceiptDigest
			case "tampered receipt":
				a.Source.ImmutableRef = b.Source.ImmutableRef
			case "wrong host":
				a.DestinationRepository = "other.fixture.invalid/fixture/recovery"
			}
			report, err := registry.VerifyRecoveryMirror(context.Background(), a, b)
			if mode != "verified" {
				if err == nil || report != (RegistryRecoveryReport{}) {
					t.Fatalf("bad mirror produced evidence: %+v: %v", report, err)
				}
				if (mode == "wrong receipt pin" || mode == "tampered receipt" || mode == "wrong host") && len(seen) != 0 {
					t.Fatalf("invalid input reached the registry: %v", seen)
				}
				return
			}
			if err != nil || seen[forwardPin] != 2 || seen[rollbackPin] != 2 || seen[forwardLayer] != 1 || seen[rollbackLayer] != 1 {
				t.Fatalf("did not verify both complete images and pins: %v: %v", seen, err)
			}
			if report.Forward.SourceImage != a.Source.ImmutableRef || report.Forward.SourceReceiptDigest != a.SourceReceiptDigest ||
				report.Rollback.SourceImage != b.Source.ImmutableRef || report.Rollback.SourceReceiptDigest != b.SourceReceiptDigest ||
				report.Forward.RecoveryImage.Image != forward.ImmutableRef || report.Rollback.RecoveryImage.Image != rollback.ImmutableRef {
				t.Fatalf("wrong bound recovery report: %+v", report)
			}
		})
	}
}
