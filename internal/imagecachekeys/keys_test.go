package imagecachekeys

import "testing"

func TestManifestReferenceKeysMatchWorkloadDigestForms(t *testing.T) {
	t.Parallel()

	repo := "fugue-apps/fugue-fugue-registry-fugue-system-svc-cluster-local-5000-fugue-apps-yym68686-argus-runtime"
	digest := "sha256:570d3b2870631111111111111111111111111111111111111111111111111111"
	liveRef := "registry.fugue.internal:5000/" + repo + "@" + digest

	live := keySet(ImageReferenceKeys(liveRef, ""))
	manifest := ManifestReferenceKeys(repo, "fugue-live-570d3b2870631", digest, "http://cache.local:5000/"+repo+":fugue-live-570d3b2870631")
	for _, required := range []string{digest, repo + "@" + digest} {
		if _, ok := live[required]; !ok {
			t.Fatalf("live ref keys missing %q: %+v", required, live)
		}
		if !containsKey(manifest, required) {
			t.Fatalf("manifest keys missing %q: %+v", required, manifest)
		}
	}
}

func TestImageReferenceKeysNormalizeRepoColonDigestForm(t *testing.T) {
	t.Parallel()

	repo := "fugue-apps/demo"
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	keys := keySet(ImageReferenceKeys(repo+":sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ""))
	for _, required := range []string{digest, repo + "@" + digest, repo + ":" + digest} {
		if _, ok := keys[required]; !ok {
			t.Fatalf("keys missing %q: %+v", required, keys)
		}
	}
}

func TestExactReferenceKeysDoNotProtectWholeRepository(t *testing.T) {
	t.Parallel()

	current := keySet(ExactImageReferenceKeys("registry.fugue.internal:5000/fugue-apps/demo:current", ""))
	oldManifest := ExactManifestReferenceKeys("fugue-apps/demo", "old", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	if _, ok := current["fugue-apps/demo"]; ok {
		t.Fatalf("exact keys must not include bare repo: %+v", current)
	}
	for _, key := range oldManifest {
		if _, ok := current[key]; ok {
			t.Fatalf("current exact key %q unexpectedly matches old manifest keys %+v", key, oldManifest)
		}
	}
}

func keySet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func containsKey(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
