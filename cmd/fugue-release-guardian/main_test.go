package main

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
