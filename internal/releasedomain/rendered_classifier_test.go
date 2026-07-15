package releasedomain

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func readManifestFixture(t *testing.T, parts ...string) []byte {
	t.Helper()
	pathParts := append([]string{"testdata"}, parts...)
	data, err := os.ReadFile(filepath.Join(pathParts...))
	if err != nil {
		t.Fatalf("read manifest fixture: %v", err)
	}
	return data
}

func testRenderedOptions() RenderedOptions {
	return RenderedOptions{
		DefaultNamespace: "fugue-system",
		Bindings:         testBindings(),
	}
}

func TestClassifyRenderedFixtures(t *testing.T) {
	spec := testOwnership(t)
	tests := []struct {
		name        string
		base        []byte
		target      []byte
		wantDomains []Domain
		wantUnknown bool
	}{
		{
			name: "zero",
			base: readManifestFixture(t, "zero", "base.yaml"), target: readManifestFixture(t, "zero", "target.yaml"),
		},
		{
			name: "node-local",
			base: readManifestFixture(t, "single-node-local", "base.yaml"), target: readManifestFixture(t, "single-node-local", "target.yaml"),
			wantDomains: []Domain{DomainNodeLocal},
		},
		{
			name: "multiple",
			base: readManifestFixture(t, "multiple", "base.yaml"), target: readManifestFixture(t, "multiple", "target.yaml"),
			wantDomains: []Domain{DomainNodeLocal, DomainAuthoritativeDNS},
		},
		{
			name: "unowned-object",
			base: readManifestFixture(t, "unknown", "base.yaml"), target: readManifestFixture(t, "unknown", "target.yaml"),
			wantUnknown: true,
		},
		{
			name: "crd",
			base: readManifestFixture(t, "crd", "base.yaml"), target: readManifestFixture(t, "crd", "target.yaml"),
			wantUnknown: true,
		},
		{
			name: "cnpg-backup-override",
			base: readManifestFixture(t, "cnpg-override", "base.yaml"), target: readManifestFixture(t, "cnpg-override", "target-backup.yaml"),
			wantDomains: []Domain{DomainBackup},
		},
		{
			name: "cnpg-control-plane-default",
			base: readManifestFixture(t, "cnpg-override", "base.yaml"), target: readManifestFixture(t, "cnpg-override", "target-control-plane.yaml"),
			wantDomains: []Domain{DomainControlPlane},
		},
		{
			name: "cnpg-multiple",
			base: readManifestFixture(t, "cnpg-override", "base.yaml"), target: readManifestFixture(t, "cnpg-override", "target-multiple.yaml"),
			wantDomains: []Domain{DomainControlPlane, DomainBackup},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifyRendered(test.base, test.target, spec, testRenderedOptions())
			if !equalDomains(got.Domains, test.wantDomains) {
				t.Fatalf("domains = %v, want %v; unknown=%#v", got.Domains, test.wantDomains, got.Unknown)
			}
			if (len(got.Unknown) > 0) != test.wantUnknown {
				t.Fatalf("unknown = %#v", got.Unknown)
			}
		})
	}
}

func TestDynamicDNSNameMustMatchComponentSuffix(t *testing.T) {
	base := []byte(`apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-dns-us-west
  namespace: fugue-system
  labels:
    app.kubernetes.io/component: dns-us-west
    fugue.io/rollout-subsystem: public-data-plane
    fugue.io/rollout-mode: direct-ondelete-protected
    fugue.io/downtime-class: online-required
spec:
  template:
    spec:
      containers:
        - name: dns
          image: edge:v1
`)
	target := []byte(strings.ReplaceAll(string(base), "edge:v1", "edge:v2"))
	got := ClassifyRendered(base, target, testOwnership(t), testRenderedOptions())
	if len(got.Unknown) != 0 || !equalDomains(got.Domains, []Domain{DomainAuthoritativeDNS}) {
		t.Fatalf("classification = %#v", got)
	}

	badTarget := []byte(strings.ReplaceAll(string(target), "app.kubernetes.io/component: dns-us-west", "app.kubernetes.io/component: dns-other"))
	bad := ClassifyRendered(base, badTarget, testOwnership(t), testRenderedOptions())
	if len(bad.Unknown) == 0 {
		t.Fatalf("expected suffix-label mismatch to be unknown: %#v", bad)
	}
}

func TestRequiredOwnershipLabelCannotBeRemoved(t *testing.T) {
	base := readManifestFixture(t, "single-node-local", "base.yaml")
	target := []byte(strings.ReplaceAll(string(readManifestFixture(t, "single-node-local", "target.yaml")), "cluster-dns", "other"))
	got := ClassifyRendered(base, target, testOwnership(t), testRenderedOptions())
	if len(got.Unknown) == 0 {
		t.Fatalf("expected label removal to fail closed: %#v", got)
	}
}

func TestStatusAndEmptyCreationTimestampAreNonSemantic(t *testing.T) {
	base := []byte(`apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-image-cache
  labels:
    app.kubernetes.io/component: image-cache
    fugue.io/rollout-subsystem: node-local-build-plane
spec:
  template:
    spec:
      containers:
        - name: image-cache
          image: cache:v1
status:
  numberReady: 1
`)
	target := []byte(strings.ReplaceAll(string(base), "numberReady: 1", "numberReady: 2"))
	target = []byte(strings.Replace(string(target), "  labels:\n", "  creationTimestamp: null\n  labels:\n", 1))
	got := ClassifyRendered(base, target, testOwnership(t), testRenderedOptions())
	if len(got.Unknown) != 0 || len(got.Domains) != 0 || len(got.Evidence) != 0 {
		t.Fatalf("classification = %#v", got)
	}
}

func TestHelmTestHookRequiresExplicitIgnore(t *testing.T) {
	target := []byte(`apiVersion: v1
kind: Pod
metadata:
  name: fugue-test
  namespace: fugue-system
  annotations:
    helm.sh/hook: test
spec:
  containers:
    - name: test
      image: busybox
`)
	options := testRenderedOptions()
	blocked := ClassifyRendered(nil, target, testOwnership(t), options)
	if len(blocked.Unknown) == 0 {
		t.Fatal("expected test hook to be blocked by default")
	}
	options.IgnoreHelmTestHooks = true
	ignored := ClassifyRendered(nil, target, testOwnership(t), options)
	if len(ignored.Unknown) != 0 || len(ignored.Evidence) != 1 || !ignored.Evidence[0].Ignored {
		t.Fatalf("ignored hook classification = %#v", ignored)
	}
}

func TestHelmHookTransitionIsNeverIgnored(t *testing.T) {
	base := []byte(`apiVersion: v1
kind: Pod
metadata:
  name: fugue-test
  namespace: fugue-system
  annotations:
    helm.sh/hook: test
spec:
  containers:
    - name: test
      image: busybox
`)
	target := []byte(strings.ReplaceAll(string(base), "    helm.sh/hook: test\n", ""))
	options := testRenderedOptions()
	options.IgnoreHelmTestHooks = true
	got := ClassifyRendered(base, target, testOwnership(t), options)
	if len(got.Unknown) == 0 {
		t.Fatalf("expected hook transition to be unknown: %#v", got)
	}
}

func TestDuplicateIdentityAndGenerateNameAreUnknown(t *testing.T) {
	object := string(readManifestFixture(t, "single-node-local", "target.yaml"))
	duplicate := ClassifyRendered(nil, []byte(object+"\n---\n"+object+"\n---\n"+object), testOwnership(t), testRenderedOptions())
	if len(duplicate.Unknown) == 0 {
		t.Fatal("expected duplicate identity to be unknown")
	}

	generated := []byte(strings.Replace(object, "  name: fugue-node-local-dns\n", "  name: fugue-node-local-dns\n  generateName: unsafe-\n", 1))
	got := ClassifyRendered(nil, generated, testOwnership(t), testRenderedOptions())
	if len(got.Unknown) == 0 {
		t.Fatal("expected generateName to be unknown")
	}
}

func TestStrictManifestRejectsDuplicateMappingKeys(t *testing.T) {
	base := readManifestFixture(t, "single-node-local", "base.yaml")
	yamlDuplicate := []byte(strings.Replace(
		string(base),
		"          image: registry.k8s.io/dns/k8s-dns-node-cache:1.26.7\n",
		"          image: registry.k8s.io/dns/k8s-dns-node-cache:1.26.7\n          image: registry.k8s.io/dns/k8s-dns-node-cache:1.26.8\n",
		1,
	))
	if got := ClassifyRendered(base, yamlDuplicate, testOwnership(t), testRenderedOptions()); len(got.Unknown) == 0 {
		t.Fatalf("expected duplicate YAML key to be unknown: %#v", got)
	}

	jsonDuplicate := []byte(`{
  "apiVersion":"apps/v1",
  "kind":"DaemonSet",
  "metadata":{
    "name":"fugue-node-local-dns",
    "namespace":"kube-system",
    "labels":{
      "app.kubernetes.io/component":"node-local-dns",
      "fugue.io/rollout-subsystem":"cluster-dns"
    }
  },
  "spec":{"template":{"spec":{"containers":[{
    "name":"node-cache",
    "image":"registry.k8s.io/dns/k8s-dns-node-cache:1.26.7",
    "image":"registry.k8s.io/dns/k8s-dns-node-cache:1.26.8"
  }]}}}
}`)
	if got := ClassifyRendered(base, jsonDuplicate, testOwnership(t), testRenderedOptions()); len(got.Unknown) == 0 {
		t.Fatalf("expected duplicate JSON key to be unknown: %#v", got)
	}
}

func TestStrictManifestRejectsExplicitTaggedEmptyNull(t *testing.T) {
	for _, manifest := range []string{
		`!!null ""`,
		`!!null`,
		`!<tag:yaml.org,2002:null> ""`,
	} {
		got := ClassifyRendered(nil, []byte(manifest), testOwnership(t), testRenderedOptions())
		if len(got.Unknown) == 0 {
			t.Fatalf("expected explicit null %q to be unknown: %#v", manifest, got)
		}
	}
}

func TestStrictManifestRejectsNonStringMetadataIdentityFields(t *testing.T) {
	base := readManifestFixture(t, "single-node-local", "base.yaml")
	for _, test := range []struct {
		name        string
		field       string
		needle      string
		replacement string
	}{
		{name: "name", field: "name", needle: "  name: fugue-node-local-dns\n", replacement: "  name: 123\n"},
		{name: "namespace", field: "namespace", needle: "  namespace: kube-system\n", replacement: "  namespace: 123\n"},
		{name: "generateName", field: "generateName", needle: "  name: fugue-node-local-dns\n", replacement: "  name: fugue-node-local-dns\n  generateName: 123\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest := []byte(strings.Replace(string(base), test.needle, test.replacement, 1))
			got := ClassifyRendered(nil, manifest, testOwnership(t), testRenderedOptions())
			if len(got.Unknown) == 0 {
				t.Fatalf("expected non-string metadata.%s to be unknown: %#v", test.field, got)
			}
		})
	}
}

func TestStrictManifestRejectsAliasesAndMerges(t *testing.T) {
	aliased := []byte(`apiVersion: apps/v1
kind: DaemonSet
metadata: &metadata
  name: fugue-node-local-dns
  namespace: kube-system
  labels:
    app.kubernetes.io/component: node-local-dns
    fugue.io/rollout-subsystem: cluster-dns
spec:
  template:
    metadata: *metadata
    spec:
      containers: []
`)
	if got := ClassifyRendered(nil, aliased, testOwnership(t), testRenderedOptions()); len(got.Unknown) == 0 {
		t.Fatalf("expected YAML aliases to be unknown: %#v", got)
	}

	merged := []byte(`apiVersion: apps/v1
kind: DaemonSet
defaults: &defaults
  namespace: kube-system
metadata:
  <<: *defaults
  name: fugue-node-local-dns
  labels:
    app.kubernetes.io/component: node-local-dns
    fugue.io/rollout-subsystem: cluster-dns
spec: {}
`)
	if got := ClassifyRendered(nil, merged, testOwnership(t), testRenderedOptions()); len(got.Unknown) == 0 {
		t.Fatalf("expected YAML merge keys to be unknown: %#v", got)
	}
}

func TestRenderedDiffPreservesIntegersAboveTwoToThe53(t *testing.T) {
	base := []byte(strings.Replace(
		string(readManifestFixture(t, "single-node-local", "base.yaml")),
		"          image: registry.k8s.io/dns/k8s-dns-node-cache:1.26.7\n",
		"          image: registry.k8s.io/dns/k8s-dns-node-cache:1.26.7\n          securityContext:\n            runAsUser: 9007199254740992\n",
		1,
	))
	target := []byte(strings.Replace(string(base), "9007199254740992", "9007199254740993", 1))
	got := ClassifyRendered(base, target, testOwnership(t), testRenderedOptions())
	if len(got.Unknown) != 0 || !equalDomains(got.Domains, []Domain{DomainNodeLocal}) {
		t.Fatalf("classification = %#v", got)
	}
	if len(got.Evidence) != 1 || !slices.Contains(got.Evidence[0].Paths, "/spec/template/spec/containers/0/securityContext/runAsUser") {
		t.Fatalf("numeric diff evidence = %#v", got.Evidence)
	}
}

func TestNullAndEmptyObjectDocumentsAreUnknown(t *testing.T) {
	for _, test := range []struct {
		name     string
		manifest []byte
	}{
		{name: "null", manifest: []byte("null\n")},
		{name: "empty-object", manifest: []byte("{}\n")},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifyRendered(nil, test.manifest, testOwnership(t), testRenderedOptions())
			if len(got.Unknown) == 0 {
				t.Fatalf("expected empty document to be unknown: %#v", got)
			}
		})
	}
}

func TestCommentOnlyDocumentsDoNotBreakHelmMultiDocumentOutput(t *testing.T) {
	base := readManifestFixture(t, "single-node-local", "base.yaml")
	target := readManifestFixture(t, "single-node-local", "target.yaml")
	commentOnly := []byte("---\n# Source: disabled-template.yaml\n# no rendered object\n---\n")
	base = append(commentOnly, base...)
	target = append(commentOnly, target...)
	got := ClassifyRendered(base, target, testOwnership(t), testRenderedOptions())
	if len(got.Unknown) != 0 || !equalDomains(got.Domains, []Domain{DomainNodeLocal}) {
		t.Fatalf("classification = %#v", got)
	}
}

func indentManifestForList(manifest []byte, spaces int) string {
	prefix := strings.Repeat(" ", spaces)
	return prefix + strings.ReplaceAll(strings.TrimSpace(string(manifest)), "\n", "\n"+prefix)
}

func coreList(manifest []byte) []byte {
	return []byte("apiVersion: v1\nkind: List\nitems:\n  - " + strings.TrimPrefix(indentManifestForList(manifest, 4), "    ") + "\n")
}

func TestCoreV1ListAndNestedListValidation(t *testing.T) {
	base := coreList(readManifestFixture(t, "single-node-local", "base.yaml"))
	target := coreList(readManifestFixture(t, "single-node-local", "target.yaml"))
	got := ClassifyRendered(base, target, testOwnership(t), testRenderedOptions())
	if len(got.Unknown) != 0 || !equalDomains(got.Domains, []Domain{DomainNodeLocal}) {
		t.Fatalf("valid List classification = %#v\n%s", got, target)
	}

	nestedBase := coreList(base)
	nestedTarget := coreList(target)
	nested := ClassifyRendered(nestedBase, nestedTarget, testOwnership(t), testRenderedOptions())
	if len(nested.Unknown) != 0 || !equalDomains(nested.Domains, []Domain{DomainNodeLocal}) {
		t.Fatalf("valid nested List classification = %#v\n%s", nested, nestedTarget)
	}

	for _, test := range []struct {
		name     string
		manifest []byte
	}{
		{name: "missing-api-version", manifest: []byte(strings.Replace(string(target), "apiVersion: v1\n", "", 1))},
		{name: "wrong-api-version", manifest: []byte(strings.Replace(string(target), "apiVersion: v1", "apiVersion: apps/v1", 1))},
		{name: "nested-wrong-api-version", manifest: []byte(strings.Replace(string(nestedTarget), "  - apiVersion: v1", "  - apiVersion: apps/v1", 1))},
	} {
		t.Run(test.name, func(t *testing.T) {
			classification := ClassifyRendered(nestedBase, test.manifest, testOwnership(t), testRenderedOptions())
			if len(classification.Unknown) == 0 {
				t.Fatalf("expected invalid List to be unknown: %#v\n%s", classification, test.manifest)
			}
		})
	}
}

func currentChartRenderedOptions() RenderedOptions {
	return RenderedOptions{
		DefaultNamespace: "fugue-system",
		Bindings: map[string]string{
			"releaseNamespace":               "fugue-system",
			"nodeLocalNamespace":             "kube-system",
			"nodeLocalName":                  "fugue-fugue-node-local-dns",
			"nodeLocalUpstreamServiceName":   "fugue-fugue-dns-upstream",
			"nodeLocalActiveName":            "fugue-fugue-node-local-dns-active",
			"dnsName":                        "fugue-fugue-dns",
			"apiName":                        "fugue-fugue-api",
			"controllerName":                 "fugue-fugue-controller",
			"serviceName":                    "fugue-fugue",
			"ingressName":                    "fugue-fugue",
			"imageCacheName":                 "fugue-fugue-image-cache",
			"controlPlanePostgresName":       "fugue-fugue-control-plane-postgres",
			"controlPlanePostgresSecretName": "fugue-fugue-control-plane-postgres-app",
			"controlPlaneRestoreDrillName":   "fugue-fugue-control-plane-restore-drill",
		},
		IgnoreHelmTestHooks: true,
	}
}

func renderCurrentChart(t *testing.T, overrides ...string) []byte {
	t.Helper()
	chartDir := filepath.Join("..", "..", "deploy", "helm", "fugue")
	args := []string{
		"template", "fugue", chartDir,
		"--namespace", "fugue-system",
		"--skip-tests",
		"--set-string", "configSecret.existingSecretName=release-domain-test-config",
		"--set-string", "platformComponentIdentity.existingSecretName=release-domain-test-identity",
		"--set", "nodeLocalDNS.enabled=true",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.43.0.10",
		"--set-string", "nodeLocalDNS.targetNodes[0]=worker-a",
		"--set", "dns.enabled=true",
		"--set-string", "dns.answerIPs[0]=203.0.113.10",
		"--set-string", "dns.zone=dns.example.test",
		"--set", "controlPlanePostgres.enabled=true",
		"--set-string", "controlPlanePostgres.password=release-domain-test-password",
		"--set", "controlPlanePostgres.backup.enabled=true",
		"--set-string", "controlPlanePostgres.backup.destinationPath=s3://release-domain-test/base",
		"--set", "controlPlanePostgres.backup.scheduled.enabled=true",
	}
	args = append(args, overrides...)
	command := exec.Command("helm", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	return output
}

func renderCurrentProductionProfile(t *testing.T) []byte {
	t.Helper()
	chartDir := filepath.Join("..", "..", "deploy", "helm", "fugue")
	args := []string{
		"template", "fugue", chartDir,
		"--namespace", "fugue-system",
		"--skip-tests",
		"-f", filepath.Join(chartDir, "values-production-ha.yaml"),
		"--set-string", "configSecret.existingSecretName=release-domain-test-config",
		"--set-string", "platformComponentIdentity.existingSecretName=release-domain-test-identity",
		"--set", "nodeLocalDNS.enabled=true",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.43.0.10",
		"--set-string", "nodeLocalDNS.targetNodes[0]=worker-a",
		"--set-string", "nodeLocalDNS.preservedOfflineNodes[0]=dmit",
		"--set-string", "nodeLocalDNS.legacyMode=iptables",
		"--set", "dns.enabled=true",
		"--set-string", "dns.answerIPs[0]=203.0.113.10",
		"--set-string", "dns.zone=dns.example.test",
		"--set-string", "dns.groups[0].name=eu",
		"--set-string", "dns.groups[0].edgeGroupID=eu",
		"--set-string", "dns.groups[0].answerIPs[0]=203.0.113.11",
		"--set-string", "dns.groups[0].nodeSelector.fugue\\.io/role\\.dns=true",
		"--set", "ingress.enabled=true",
		"--set", "controlPlanePostgres.enabled=true",
		"--set-string", "controlPlanePostgres.password=release-domain-test-password",
		"--set", "controlPlanePostgres.backup.enabled=true",
		"--set-string", "controlPlanePostgres.backup.destinationPath=s3://release-domain-test/base",
		"--set", "controlPlanePostgres.backup.scheduled.enabled=true",
		"--set", "controlPlanePostgres.restoreDrill.enabled=true",
		"--set-literal", "controlPlanePostgres.restoreDrill.restoreManifestJSON={}",
	}
	command := exec.Command("helm", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("production-profile helm template failed: %v\n%s", err, output)
	}
	return output
}

func TestCurrentProductionProfileExercisesEveryObjectOwnershipRule(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	spec := testOwnership(t)
	manifest := renderCurrentProductionProfile(t)
	rendered := ClassifyRendered(nil, manifest, spec, currentChartRenderedOptions())

	seen := make(map[string]int, len(spec.ObjectRules))
	for _, evidence := range rendered.Evidence {
		if evidence.Ignored || evidence.RuleID == "" {
			continue
		}
		seen[evidence.RuleID]++
	}
	missing := make([]string, 0)
	for _, rule := range spec.ObjectRules {
		if seen[rule.ID] == 0 {
			missing = append(missing, rule.ID)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("production-profile render did not exercise ownership rules %v; evidence=%#v unknown=%#v", missing, rendered.Evidence, rendered.Unknown)
	}
	if !equalDomains(rendered.Domains, KnownDomains()) {
		t.Fatalf("production-profile rendered domains = %v, want all five; unknown=%#v", rendered.Domains, rendered.Unknown)
	}
}

func classifyCurrentValue(t *testing.T, spec *OwnershipSpec, pointer string) FileClassification {
	t.Helper()
	return ClassifyFiles([]ChangedFile{{
		Status:        ChangeModified,
		Path:          "deploy/helm/fugue/values.yaml",
		ValuePointers: []string{pointer},
	}}, spec)
}

func TestCurrentChartSingleDomainValueRenderCoverage(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	spec := testOwnership(t)
	base := renderCurrentChart(t)
	tests := []struct {
		name      string
		pointer   string
		domain    Domain
		overrides []string
	}{
		{
			name: "node-local", pointer: "/nodeLocalDNS/minReadySeconds", domain: DomainNodeLocal,
			overrides: []string{"--set", "nodeLocalDNS.minReadySeconds=11"},
		},
		{
			name: "authoritative-dns", pointer: "/dns/containerName", domain: DomainAuthoritativeDNS,
			overrides: []string{"--set-string", "dns.containerName=release-domain-dns"},
		},
		{
			name: "control-plane", pointer: "/api/image/tag", domain: DomainControlPlane,
			overrides: []string{"--set-string", "api.image.tag=release-domain-test"},
		},
		{
			name: "image-cache", pointer: "/imageCache/image/tag", domain: DomainImageCache,
			overrides: []string{"--set-string", "imageCache.image.tag=release-domain-test"},
		},
		{
			name: "backup", pointer: "/controlPlanePostgres/backup/destinationPath", domain: DomainBackup,
			overrides: []string{"--set-string", "controlPlanePostgres.backup.destinationPath=s3://release-domain-test/target"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := classifyCurrentValue(t, spec, test.pointer)
			if len(files.Unknown) != 0 || !equalDomains(files.Domains, []Domain{test.domain}) {
				t.Fatalf("value classification = %#v", files)
			}

			target := renderCurrentChart(t, test.overrides...)
			rendered := ClassifyRendered(base, target, spec, currentChartRenderedOptions())
			if len(rendered.Unknown) != 0 || !equalDomains(rendered.Domains, []Domain{test.domain}) {
				t.Fatalf("rendered classification = %#v", rendered)
			}
		})
	}
}

func TestCurrentChartCrossDomainValuesFailClosed(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	spec := testOwnership(t)
	base := renderCurrentChart(t)
	tests := []struct {
		name        string
		pointer     string
		overrides   []string
		wantDomains []Domain
		wantUnknown bool
	}{
		{
			name: "dns-ttl", pointer: "/dns/ttl",
			overrides:   []string{"--set", "dns.ttl=61"},
			wantDomains: []Domain{DomainAuthoritativeDNS, DomainControlPlane},
		},
		{
			name: "image-cache-port", pointer: "/imageCache/port",
			overrides:   []string{"--set", "imageCache.port=5001"},
			wantDomains: []Domain{DomainControlPlane, DomainImageCache},
		},
		{
			name: "controller-image", pointer: "/controller/image/tag",
			overrides:   []string{"--set-string", "controller.image.tag=release-domain-test"},
			wantDomains: []Domain{DomainControlPlane}, wantUnknown: true,
		},
		{
			name: "backup-enabled", pointer: "/controlPlanePostgres/backup/enabled",
			overrides:   []string{"--set", "controlPlanePostgres.backup.enabled=false"},
			wantDomains: []Domain{DomainControlPlane, DomainBackup},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := classifyCurrentValue(t, spec, test.pointer)
			if len(files.Unknown) == 0 || len(files.Domains) != 0 {
				t.Fatalf("cross-domain value did not fail closed: %#v", files)
			}

			target := renderCurrentChart(t, test.overrides...)
			rendered := ClassifyRendered(base, target, spec, currentChartRenderedOptions())
			if !equalDomains(rendered.Domains, test.wantDomains) {
				t.Fatalf("rendered domains = %v, want %v; unknown=%#v", rendered.Domains, test.wantDomains, rendered.Unknown)
			}
			if (len(rendered.Unknown) > 0) != test.wantUnknown {
				t.Fatalf("rendered unknown = %#v", rendered.Unknown)
			}
		})
	}
}

func TestCurrentChartNodeLocalEnableAlsoChangesObservability(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	spec := testOwnership(t)
	files := classifyCurrentValue(t, spec, "/nodeLocalDNS/enabled")
	if len(files.Unknown) == 0 || len(files.Domains) != 0 {
		t.Fatalf("shared value did not fail closed: %#v", files)
	}

	metrics := []string{"--set", "observability.metrics.enabled=true"}
	baseArgs := append(append([]string(nil), metrics...), "--set", "nodeLocalDNS.enabled=false")
	targetArgs := append(append([]string(nil), metrics...), "--set", "nodeLocalDNS.enabled=true")
	base := renderCurrentChart(t, baseArgs...)
	target := renderCurrentChart(t, targetArgs...)
	rendered := ClassifyRendered(base, target, spec, currentChartRenderedOptions())
	if !equalDomains(rendered.Domains, []Domain{DomainNodeLocal}) || len(rendered.Unknown) == 0 {
		t.Fatalf("rendered classification = %#v", rendered)
	}
}
