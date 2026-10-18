package releaseguardian

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestWaitForTerminalCommittedTargetRequiresFreshHealthyEvidence(t *testing.T) {
	for _, scenario := range []string{"recovers", "still degraded at deadline", "candidate not committed", "previous LKG"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			key := Key{Component: "api", Group: "global"}
			expected := DesiredRelease{APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: key.Component, Group: key.Group, RecordDigest: testDigest, Generation: 2}
			initial := ReleaseStatus{Component: key.Component, Group: key.Group, State: StateRecoveryRequired,
				CurrentRecordDigest: testDigest, TargetRecordDigest: testDigest, LastSuccessfulLKG: testDigest,
				Health: testHealth(HealthDegraded, HealthHealthy, HealthUnknown, now), Reason: "health evidence is incomplete", ObservedAt: now.Format(time.RFC3339Nano)}
			if scenario == "candidate not committed" {
				initial.CurrentRecordDigest = otherDigest
			}
			if scenario == "previous LKG" {
				initial.LastSuccessfulLKG = otherDigest
			}
			client := kubernetesfake.NewSimpleClientset()
			reads := 0
			client.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.(clienttesting.GetAction).GetName() != statusName(key) {
					return false, nil, nil
				}
				reads++
				value := initial
				if scenario == "recovers" && reads > 1 {
					value.State, value.Health = StateStable, testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now)
				}
				sealed, err := value.Seal()
				if err != nil {
					return true, nil, err
				}
				raw, err := declarativerelease.CanonicalJSON(sealed)
				return true, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: statusName(key), Namespace: "fugue-system"}, Data: map[string]string{"status.json": string(raw)}}, err
			})
			store, err := NewKubeStore(client, []TargetConfig{{Key: key, Namespace: "fugue-system", MonitorComponent: key.Component, DependencyService: "dependency"}})
			if err != nil {
				t.Fatal(err)
			}
			store.now = func() time.Time { return now }
			limit := 3 * time.Second
			if scenario != "recovers" {
				limit = 50 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), limit)
			defer cancel()
			result, err := store.WaitForTerminal(ctx, key, expected, time.Second)
			switch scenario {
			case "recovers":
				if err != nil || result.State != StateStable || reads != 2 {
					t.Fatal("committed target did not wait for healthy evidence", result, err, reads)
				}
			case "still degraded at deadline":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("unhealthy committed target was treated as success", err)
				}
			default:
				if err == nil || !strings.Contains(err.Error(), "Guardian release ended in recovery_required") || reads != 1 {
					t.Fatal("uncommitted recovery stopped failing immediately", err, reads)
				}
			}
		})
	}
}
