package releaseguardian

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func TestNativeReleaseShadowUsesOnlyReadsAndRefusesLiveDrift(t *testing.T) {
	for _, mode := range []string{"same target", "changed resource version", "late replacement"} {
		t.Run(mode, func(t *testing.T) {
			f := healthyPublicationFixture(t)
			ctx := context.Background()
			reads := 0
			f.client.PrependReactor("get", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
				reads++
				if mode == "same target" || (mode == "late replacement" && reads < 5) {
					return false, nil, nil
				}
				get := action.(clienttesting.GetAction)
				object, err := f.client.Tracker().Get(action.GetResource(), action.GetNamespace(), get.GetName(), metav1.GetOptions{})
				if err != nil {
					return true, nil, err
				}
				changed := object.(*appsv1.Deployment).DeepCopy()
				if mode == "late replacement" {
					changed.UID = "new-uid"
				} else {
					changed.ResourceVersion = "11"
				}
				return true, changed, nil
			})
			f.client.ClearActions()
			result, err := f.store.CompareNativeReleaseShadow(ctx, f.key, "fixture", "request-a", f.candidate)
			if mode != "same target" {
				if err == nil || !strings.Contains(err.Error(), "prewrite binding") || result.EvidenceDigest != "" {
					t.Fatalf("replacement or stale prewrite accepted: %+v, %v", result, err)
				}
			} else {
				if err != nil || result.DeploymentUID != "uid" || result.Legacy != result.Native || !digestPattern.MatchString(result.IntentDigest) {
					t.Fatalf("shadow mismatch on identical target: %+v, %v", result, err)
				}
				want := result.EvidenceDigest
				result.EvidenceDigest = ""
				raw, err := declarativerelease.CanonicalJSON(result)
				if err != nil || digest(raw) != want {
					t.Fatal("the shadow receipt does not bind its observations")
				}
			}
			for _, action := range f.client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("shadow wrote to Kubernetes: %v", action)
				}
			}
		})
	}
}

func TestShadowComparisonIgnoresOnlyReadClockStamps(t *testing.T) {
	now := time.Now().UTC()
	before := PublicationPreview{Health: testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now), RollbackManifestDigest: testDigest}
	after := before
	after.Health.Local.ObservedAt, after.Health.Dependency.ObservedAt = now.Add(time.Second).Format(time.RFC3339Nano), now.Add(2*time.Second).Format(time.RFC3339Nano)
	if !equivalentShadowPreviews(before, after) {
		t.Fatal("the read clock is not a change in the live snapshot")
	}
	after.Health.Route.ObservedAt = after.Health.Local.ObservedAt
	if equivalentShadowPreviews(before, after) {
		t.Fatal("a replaced independent route receipt was ignored")
	}
	after = before
	after.RollbackManifestDigest = otherDigest
	if equivalentShadowPreviews(before, after) {
		t.Fatal("a different rollback manifest was ignored")
	}
}
