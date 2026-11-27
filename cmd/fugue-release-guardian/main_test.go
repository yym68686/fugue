package main

import (
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/releaseguardian"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDedicatedNativeGuardianDeclinesLegacyAuthorityRuntime(t *testing.T) {
	for _, name := range []string{
		releaseguardian.NativeRuntimeRegistryEnv, releaseguardian.NativeRuntimeAuthFileEnv, releaseguardian.NativeRuntimeBindingEnv, nativeJournalFileEnv,
		"FUGUE_RELEASE_GUARDIAN_AUTHORITY_PUBLIC_KEY_SOURCE", "FUGUE_RELEASE_GUARDIAN_CANDIDATE_IMPORTS",
		"FUGUE_RELEASE_GUARDIAN_AUTHORITY_GROUPS", "FUGUE_RELEASE_GUARDIAN_AUTHORITY_ACTIVATORS",
	} {
		t.Setenv(name, "")
	}
	if enabled, err := nativeGuardianConfigured(releaseguardian.ModeWrite, 2); enabled || err != nil {
		t.Fatalf("legacy configuration: enabled=%v err=%v", enabled, err)
	}
	t.Setenv(nativeJournalFileEnv, "/native/journal.json")
	if _, err := nativeGuardianConfigured(releaseguardian.ModeWrite, 1); err == nil {
		t.Fatal("journal settings silently fell back into the legacy runtime")
	}
	t.Setenv(releaseguardian.NativeRuntimeRegistryEnv, "registry.fixture.invalid")
	if enabled, err := nativeGuardianConfigured(releaseguardian.ModeWrite, 1); !enabled || err != nil {
		t.Fatalf("dedicated configuration: enabled=%v err=%v", enabled, err)
	}
	if _, err := nativeGuardianConfigured(releaseguardian.ModeWrite, 2); err == nil {
		t.Fatal("dedicated native writer accepted multiple targets")
	}
	if _, err := nativeGuardianConfigured(releaseguardian.ModeShadow, 1); err == nil {
		t.Fatal("dedicated native writer started as a shadow status writer")
	}
	t.Setenv("FUGUE_RELEASE_GUARDIAN_CANDIDATE_IMPORTS", "fixture")
	if _, err := nativeGuardianConfigured(releaseguardian.ModeWrite, 1); err == nil {
		t.Fatal("dedicated native writer enabled a legacy candidate importer")
	}
}

func TestNativeJournalConfigRequiresAnAuthenticatedSingleHost(t *testing.T) {
	for _, env := range []string{"PGSERVICE", "PGSSLMODE", "PGSSLROOTCERT", "PGSSLKEY", "PGSSLCERT", "PGREQUIRESSL", "PGTARGETSESSIONATTRS"} {
		t.Setenv(env, "")
	}
	const database = "postgres://launcher@db.fixture.invalid/fugue?sslmode="
	key := make([]byte, ed25519.PublicKeySize)
	key[0] = 1
	for _, tc := range []struct {
		name, dsn, extra string
		accept           bool
	}{
		{name: "authenticated endpoint", dsn: database + "verify-full", accept: true},
		{name: "no TLS", dsn: database + "disable"},
		{name: "optional TLS", dsn: database + "prefer"},
		{name: "no certificate validation", dsn: database + "require"},
		{name: "no hostname validation", dsn: database + "verify-ca"},
		{name: "multiple endpoints", dsn: "postgres://launcher@db-a.fixture.invalid,db-b.fixture.invalid/fugue?sslmode=verify-full"},
		{name: "arbitrary setting", dsn: database + "verify-full", extra: `,"unreviewed":true`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{"cluster": "fixture", "database": tc.dsn, "ownerPublicKeys": map[string][]byte{"owner-a": key}})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "journal.json")
			if err := os.WriteFile(path, []byte(strings.TrimSuffix(string(raw), "}")+tc.extra+"}"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, keys, cluster, err := loadNativeJournalConfig(path)
			if (err == nil) != tc.accept {
				t.Fatalf("accepted=%t error=%v", err == nil, err)
			}
			if tc.accept && (cluster != "fixture" || keys["owner-a"].ClusterID != cluster || !keys["owner-a"].PublicKey.Equal(ed25519.PublicKey(key)) || cfg.TLSConfig.ServerName != "db.fixture.invalid") {
				t.Fatal("did not preserve the exact journal and approval trust settings")
			}
		})
	}
}

func TestParseTargetsIsDataDrivenAndGroupScoped(t *testing.T) {
	targets, err := parseTargets("edge-control,edge-pool-a,fugue-system,edge-control-alpha,fugue-fugue;edge-control,edge-pool-b,fugue-system,edge-control-beta,fugue-fugue")
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].Key.String() == targets[1].Key.String() {
		t.Fatalf("targets=%+v", targets)
	}
}

func TestHistoricalArtifactEventsDoNotRequeueRuntimeReconciliation(t *testing.T) {
	for _, fixture := range []struct {
		name    string
		manager string
		want    bool
	}{
		{"fugue-route-bundle-record-edge-pool-a-deadbeef", "fugue-release-guardian", true},
		{"fugue-guardian-record-api-global-deadbeef", "fugue-release-guardian", true},
		{"fugue-guardian-execution-api-global-12", "fugue-release-guardian", true},
		{"fugue-release-record-api-deadbeef", "fugue-declarative-release", true},
		{"fugue-desired-release-api-global", "fugue-release-guardian", false},
		{"fugue-current-authority-edge-pool-a", "fugue-release-guardian", false},
	} {
		object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: fixture.name, Labels: map[string]string{"app.kubernetes.io/managed-by": fixture.manager}}}
		if got := isHistoricalArtifactEvent(object); got != fixture.want {
			t.Fatalf("name=%s manager=%s got=%t want=%t", fixture.name, fixture.manager, got, fixture.want)
		}
	}
}

func TestParseArtifactRetentionConfigIsBoundedAndDataDriven(t *testing.T) {
	t.Setenv("FUGUE_RELEASE_GUARDIAN_ARTIFACT_MINIMUM_AGE", "36h")
	t.Setenv("FUGUE_RELEASE_GUARDIAN_ARTIFACT_MINIMUM_HISTORY", "48")
	t.Setenv("FUGUE_RELEASE_GUARDIAN_ARTIFACT_MAXIMUM_DELETES", "256")
	t.Setenv("FUGUE_RELEASE_GUARDIAN_ARTIFACT_PRUNE_INTERVAL", "2m")
	config, err := parseArtifactRetentionConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.Policy.MinimumAge != 36*time.Hour || config.Policy.MinimumHistory != 48 || config.Policy.MaximumDeletes != 256 || config.Interval != 2*time.Minute {
		t.Fatalf("config=%+v", config)
	}
	for name, value := range map[string]string{
		"FUGUE_RELEASE_GUARDIAN_ARTIFACT_MINIMUM_AGE":     "30m",
		"FUGUE_RELEASE_GUARDIAN_ARTIFACT_MINIMUM_HISTORY": "7",
		"FUGUE_RELEASE_GUARDIAN_ARTIFACT_MAXIMUM_DELETES": "1025",
		"FUGUE_RELEASE_GUARDIAN_ARTIFACT_PRUNE_INTERVAL":  "10s",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := parseArtifactRetentionConfig(); err == nil {
				t.Fatalf("accepted %s=%s", name, value)
			}
		})
	}
}

func TestParseCanaryRejectsUnboundedOrMalformedInput(t *testing.T) {
	if _, err := parseProbe("edge-control,edge-pool-a,192.0.2.1:443,platform.example,/healthz,ok,10"); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		"edge-control,edge-pool-a,192.0.2.1:443,platform.example,/healthz,ok,1",
		"edge-control,edge-pool-a,192.0.2.1:443,platform.example,healthz,ok,10",
		"edge-control,edge-pool-a,192.0.2.1:443,platform.example,/healthz,ok\nleak,10",
	} {
		if _, err := parseProbe(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestParseCanariesIsDataDrivenAndRejectsDuplicateTargets(t *testing.T) {
	probes, err := parseProbes("edge-control-de,de,192.0.2.1:443,platform.example,/healthz,ok,10;edge-worker-de,de,192.0.2.1:443,platform.example,/healthz,ok,10")
	if err != nil || len(probes) != 2 || probes[0].Key == probes[1].Key {
		t.Fatalf("probes=%+v err=%v", probes, err)
	}
	if _, err := parseProbes("edge-control-de,de,192.0.2.1:443,platform.example,/healthz,ok,10;edge-control-de,de,192.0.2.1:443,platform.example,/healthz,ok,10"); err == nil {
		t.Fatal("duplicate canary target was accepted")
	}
}
