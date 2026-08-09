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

func TestArtifactImageOwnershipRepairPreservesDeclarativeSiblingFields(t *testing.T) {
	lkg := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"labels":{"app":"edge"},"name":"edge-alpha-worker","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"edge"}},"template":{"metadata":{"labels":{"app":"edge"}},"spec":{"containers":[{"image":"example/edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"edge","volumeMounts":[{"mountPath":"/state","name":"worker-state"}]}],"initContainers":[{"image":"example/edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"edge-workload-identity","volumeMounts":[{"mountPath":"/identity","name":"identity"}]}],"volumes":[{"emptyDir":{},"name":"worker-state"},{"emptyDir":{},"name":"identity"}]}}}}],"kind":"ComponentResourceSet"}`)
	plan := OwnershipAdoptionPlan{
		BootstrapLKGDigest: digestOf(lkg),
		Resources: []OwnershipAdoptionResourcePlan{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-alpha-worker"},
			Fields: []string{
				"/spec/template/spec/containers[name=edge]/image",
				"/spec/template/spec/initContainers[name=edge-workload-identity]/image",
			},
			UID: "worker-uid", ResourceVersion: "42", Generation: 7,
		}},
	}
	raw, err := BuildOwnershipAdoptionManifest(lkg, plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"uid":"worker-uid"`, `"resourceVersion":"42"`, `"selector"`, `"volumeMounts"`, `"volumes"`} {
		if !bytes.Contains(raw, []byte(required)) {
			t.Fatalf("image ownership repair lost declarative sibling %s: %s", required, raw)
		}
	}
	plan.Resources[0].Fields = append(plan.Resources[0].Fields, "/spec/template/spec/volumes")
	raw, err = BuildOwnershipAdoptionManifest(lkg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"selector"`)) || bytes.Contains(raw, []byte(`"labels"`)) {
		t.Fatalf("non-image adoption unexpectedly claimed the full resource: %s", raw)
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

func TestOwnershipContainerManifestKeepsOnlyNameAndImage(t *testing.T) {
	bootstrap := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"template":{"spec":{"containers":[{"env":[{"name":"MODE","value":"bootstrap"}],"image":"example/api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","livenessProbe":{"httpGet":{"path":"/healthz","port":"http"}},"name":"api","ports":[{"containerPort":8080,"name":"http"}],"resources":{"requests":{"cpu":"100m"}},"volumeMounts":[{"mountPath":"/secret","name":"tls"}]}],"volumes":[{"name":"tls","secret":{"secretName":"api-tls"}}]}}}}],"kind":"ComponentResourceSet"}`)
	forward := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"template":{"spec":{"containers":[{"env":[{"name":"MODE","value":"forward"}],"image":"example/api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"api","ports":[{"containerPort":9090,"name":"metrics"}],"readinessProbe":{"httpGet":{"path":"/readyz","port":"metrics"}},"resources":{"limits":{"cpu":"1"}},"volumeMounts":[{"mountPath":"/other","name":"config"}]}],"volumes":[{"configMap":{"name":"api-config"},"name":"config"}]}}}}],"kind":"ComponentResourceSet"}`)
	plan := OwnershipAdoptionPlan{
		BootstrapLKGDigest: digestOf(bootstrap), ImageRef: "example/edge@sha256:" + strings.Repeat("a", 64),
		Resources: []OwnershipAdoptionResourcePlan{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
			Fields:   []string{"/spec/template/spec/containers"}, UID: "api-uid", ResourceVersion: "42", Generation: 7,
		}},
	}
	forwardIdentity := TargetIdentity{
		Present: true, ImageRef: "example/api@sha256:" + strings.Repeat("b", 64), ConfigSHA: strings.Repeat("2", 40),
		ManifestSHA: strings.Repeat("2", 40), OCIRevision: strings.Repeat("2", 40), ManifestDigest: digestOf(forward),
	}
	tests := []struct {
		name      string
		wantImage string
		build     func() ([]byte, error)
	}{
		{name: "bootstrap", wantImage: strings.Repeat("a", 64), build: func() ([]byte, error) {
			return BuildOwnershipAdoptionManifest(bootstrap, plan)
		}},
		{name: "takeover", wantImage: strings.Repeat("b", 64), build: func() ([]byte, error) {
			plan.AlreadyConverged = true
			return BuildOwnershipTakeoverManifest(forward, plan, forwardIdentity)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := test.build()
			if err != nil {
				t.Fatal(err)
			}
			want := `"containers":[{"image":"example/api@sha256:` + test.wantImage + `","name":"api"}]`
			if !bytes.Contains(raw, []byte(want)) {
				t.Fatalf("ownership-scoped container lost name/image: %s", raw)
			}
			for _, forbidden := range []string{`"env"`, `"ports"`, `"resources"`, `"livenessProbe"`, `"readinessProbe"`, `"volumeMounts"`, `"volumes"`} {
				if bytes.Contains(raw, []byte(forbidden)) {
					t.Fatalf("ownership-scoped container copied %s: %s", forbidden, raw)
				}
			}
		})
	}
}

func TestOwnershipAssociativePointerClaimsOnlyOneEnvironmentValue(t *testing.T) {
	bootstrap := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"fugue-fugue-dns-country-de","namespace":"fugue-system"},"spec":{"template":{"spec":{"containers":[{"env":[{"name":"FUGUE_API_URL","value":"https://api.fugue.pro/v1/edge/dns"},{"name":"OTHER","value":"unchanged"}],"image":"example/edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"dns","volumeMounts":[{"mountPath":"/cache","name":"cache"}]},{"image":"example/sidecar@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","name":"sidecar"}],"volumes":[{"emptyDir":{},"name":"cache"}]}}}}],"kind":"ComponentResourceSet"}`)
	forward := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"fugue-fugue-dns-country-de","namespace":"fugue-system"},"spec":{"template":{"spec":{"containers":[{"env":[{"name":"FUGUE_API_URL","value":"https://api.fugue.pro/v1/edge/dns?authority_service=edge-control-de"},{"name":"OTHER","value":"changed-but-unowned"}],"image":"example/edge@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"dns"},{"image":"example/sidecar@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","name":"sidecar"}]}}}}],"kind":"ComponentResourceSet"}`)
	pointer := "/spec/template/spec/containers[name=dns]/env[name=FUGUE_API_URL]/value"
	plan := OwnershipAdoptionPlan{
		BootstrapLKGDigest: digestOf(bootstrap), ImageRef: "example/edge@sha256:" + strings.Repeat("a", 64),
		Resources: []OwnershipAdoptionResourcePlan{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-dns-country-de"},
			Fields:   []string{pointer}, ValidationScaffolds: []OwnershipValidationScaffold{{
				Pointer: "/spec/template/spec/containers[name=dns]/image", Value: "example/edge@sha256:" + strings.Repeat("a", 64),
			}}, UID: "dns-uid", ResourceVersion: "42", Generation: 7,
		}},
	}
	identity := TargetIdentity{Present: true, ImageRef: "example/edge@sha256:" + strings.Repeat("b", 64),
		ConfigSHA: strings.Repeat("2", 40), ManifestSHA: strings.Repeat("2", 40), OCIRevision: strings.Repeat("2", 40), ManifestDigest: digestOf(forward)}
	tests := []struct {
		name, value  string
		wantScaffold bool
		build        func() ([]byte, error)
	}{
		{name: "bootstrap", value: "https://api.fugue.pro/v1/edge/dns", wantScaffold: false, build: func() ([]byte, error) {
			return BuildOwnershipAdoptionManifest(bootstrap, plan)
		}},
		{name: "takeover", value: "https://api.fugue.pro/v1/edge/dns?authority_service=edge-control-de", wantScaffold: true, build: func() ([]byte, error) {
			plan.AlreadyConverged = true
			return BuildOwnershipTakeoverManifest(forward, plan, identity)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := test.build()
			if err != nil {
				t.Fatal(err)
			}
			image := ""
			if test.wantScaffold {
				image = `,"image":"example/edge@sha256:` + strings.Repeat("a", 64) + `"`
			}
			want := `"containers":[{"env":[{"name":"FUGUE_API_URL","value":"` + test.value + `"}]` + image + `,"name":"dns"}]`
			if !bytes.Contains(raw, []byte(want)) {
				t.Fatalf("ownership manifest did not contain the exact associative leaf: %s", raw)
			}
			for _, forbidden := range []string{`"OTHER"`, `"sidecar"`, `"volumeMounts"`, `"volumes"`} {
				if bytes.Contains(raw, []byte(forbidden)) {
					t.Fatalf("ownership manifest claimed unreviewed %s: %s", forbidden, raw)
				}
			}
			if !test.wantScaffold && bytes.Contains(raw, []byte(`"image"`)) {
				t.Fatalf("equal-value adoption unexpectedly included a takeover validation scaffold: %s", raw)
			}
		})
	}

	plan.AlreadyConverged = true
	plan.Resources[0].Fields = []string{pointer, "/spec/template/spec/containers[name=dns]/image"}
	raw, err := BuildOwnershipTakeoverManifest(forward, plan, identity)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"image":"example/edge@sha256:`+strings.Repeat("b", 64)+`"`)) ||
		bytes.Contains(raw, []byte(`"image":"example/edge@sha256:`+strings.Repeat("a", 64)+`"`)) {
		t.Fatalf("reviewed image was overwritten by its LKG validation scaffold: %s", raw)
	}

	for _, invalid := range []string{
		"/spec/template/spec/containers[name=missing]/env[name=FUGUE_API_URL]/value",
		"/spec/template/spec/containers[name=dns]/env[name=]/value",
		"/spec/template/spec/containers[name=dns]/env[name=FUGUE_API_URL]",
	} {
		plan.AlreadyConverged = false
		plan.Resources[0].Fields = []string{invalid}
		if _, err := BuildOwnershipAdoptionManifest(bootstrap, plan); err == nil {
			t.Fatalf("invalid associative pointer %q was accepted", invalid)
		}
	}
}

func TestBindOwnershipValidationScaffoldsUsesTheReviewedAuxiliaryLKG(t *testing.T) {
	lkg := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"dns-de","namespace":"fugue-system"},"spec":{"template":{"spec":{"containers":[{"image":"example/edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"dns"}]}}}},{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"ssh-de","namespace":"fugue-system"},"spec":{"template":{"spec":{"containers":[{"image":"example/edge@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"ssh-front"}]}}}}],"kind":"ComponentResourceSet"}`)
	scope := OwnershipAdoptionScope{
		Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "dns-de"},
		Fields:   []string{"/spec/template/spec/containers[name=dns]/env[name=FUGUE_API_URL]/value"},
	}
	scaffolds, err := bindOwnershipValidationScaffolds(lkg, scope)
	if err != nil || len(scaffolds) != 1 || scaffolds[0].Pointer != "/spec/template/spec/containers[name=dns]/image" ||
		scaffolds[0].Value != "example/edge@sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("auxiliary scaffold was not bound to its own LKG resource: scaffolds=%+v err=%v", scaffolds, err)
	}
	scope.Identity.Name = "missing"
	if _, err := bindOwnershipValidationScaffolds(lkg, scope); err == nil {
		t.Fatal("missing auxiliary LKG resource was accepted")
	}
}
