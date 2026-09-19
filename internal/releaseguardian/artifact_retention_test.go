package releaseguardian

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func TestArtifactPrunerRetainsCurrentRouteAndBoundedHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	group := "edge-pool-a"
	objects := make([]runtime.Object, 0, 11)
	var oldest RouteBundleRecord
	for index := 0; index < 10; index++ {
		record := sealedRouteRecord(t, group, int64(index+1))
		if index == 0 {
			oldest = record
		}
		raw, err := declarativerelease.CanonicalJSON(record)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, retentionConfigMap(routeBundleRecordName(group, record.RecordDigest), now.Add(time.Duration(index-48)*time.Hour), authorityLabels(group), map[string]string{
			"fugue.pro/authority-kind": "route-bundle",
		}, map[string]string{"record.json": string(raw)}, true))
	}
	authority := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group, CurrentRecordDigest: oldest.RecordDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 1}
	authorityRaw, _ := declarativerelease.CanonicalJSON(authority)
	objects = append(objects, retentionConfigMap(currentAuthorityName(group), now, authorityLabels(group), nil, map[string]string{"authority.json": string(authorityRaw)}, false))

	client := fake.NewSimpleClientset(objects...)
	pruner, err := NewArtifactPruner(client, "fugue-system", ArtifactRetentionPolicy{MinimumAge: time.Hour, MinimumHistory: 8, MaximumDeletes: 2})
	if err != nil {
		t.Fatal(err)
	}
	result, err := pruner.Prune(ctx, now)
	if err != nil || result.Candidates != 1 || result.Deleted != 1 || result.Remaining != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, routeBundleRecordName(group, oldest.RecordDigest), metav1.GetOptions{}); err != nil {
		t.Fatalf("current route record was pruned: %v", err)
	}
}

func TestArtifactPrunerKeepsMonitorReferencedByRetainedGuardianRecord(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	key := Key{Component: "edge-control-de", Group: "de"}
	objects := make([]runtime.Object, 0, 12)
	monitorNames := make([]string, 0, 10)
	monitorRecords := make([]declarativerelease.MonitorRecord, 0, 10)
	for index := 0; index < 10; index++ {
		data, monitor, _, _ := guardianStableFixture(t, key, now.Add(time.Duration(index-48)*time.Hour))
		name := monitorRecordNameFromDigest(key.Component, monitor.RecordDigest)
		monitorNames = append(monitorNames, name)
		monitorRecords = append(monitorRecords, monitor)
		objects = append(objects, retentionConfigMap(name, now.Add(time.Duration(index-48)*time.Hour), map[string]string{
			"app.kubernetes.io/managed-by": "fugue-declarative-release",
			"fugue.pro/component":          key.Component,
			"fugue.pro/config-sha":         monitor.ConfigSHA,
		}, nil, data, true))
	}
	state, _, err := declarativerelease.NewMonitorState(monitorRecords[9], declarativerelease.MonitorState{}, true, "", now)
	if err != nil {
		t.Fatal(err)
	}
	stateRaw, _ := declarativerelease.CanonicalJSON(state)
	objects = append(objects, retentionConfigMap("fugue-release-monitor-"+key.Component, now, map[string]string{
		"app.kubernetes.io/managed-by": "fugue-declarative-release",
		"fugue.pro/component":          key.Component,
		"fugue.pro/config-sha":         state.ConfigSHA,
	}, nil, map[string]string{"recordName": monitorNames[9], "state.json": string(stateRaw)}, false))

	guardian, err := NewReleaseRecord(key, testSHA, testDigest, testDigest, otherDigest, testDigest)
	if err != nil {
		t.Fatal(err)
	}
	guardianRaw, _ := declarativerelease.CanonicalJSON(guardian)
	objects = append(objects, retentionConfigMap(releaseRecordName(key, guardian.RecordDigest), now.Add(-48*time.Hour), guardianLabels(key), nil, map[string]string{
		"guardian-record.json":      string(guardianRaw),
		"lkg-monitor-record-digest": monitorRecords[0].RecordDigest,
	}, true))

	client := fake.NewSimpleClientset(objects...)
	pruner, err := NewArtifactPruner(client, "fugue-system", ArtifactRetentionPolicy{MinimumAge: time.Hour, MinimumHistory: 8, MaximumDeletes: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, err := pruner.Prune(ctx, now)
	if err != nil || result.Candidates != 1 || result.Deleted != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for _, protected := range []string{monitorNames[0], monitorNames[9], releaseRecordName(key, guardian.RecordDigest)} {
		if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, protected, metav1.GetOptions{}); err != nil {
			t.Fatalf("protected artifact %s was pruned: %v", protected, err)
		}
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, monitorNames[1], metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired unreferenced monitor remains: %v", err)
	}
}

func TestArtifactPrunerFailsClosedBeforeDeletingInvalidInventory(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	group := "edge-pool-a"
	objects := make([]runtime.Object, 0, 10)
	var candidateName string
	for index := 0; index < 9; index++ {
		record := sealedRouteRecord(t, group, int64(index+1))
		raw, _ := declarativerelease.CanonicalJSON(record)
		name := routeBundleRecordName(group, record.RecordDigest)
		if index == 0 {
			candidateName = name
		}
		objects = append(objects, retentionConfigMap(name, now.Add(time.Duration(index-48)*time.Hour), authorityLabels(group), map[string]string{
			"fugue.pro/authority-kind": "route-bundle",
		}, map[string]string{"record.json": string(raw)}, true))
	}
	objects = append(objects, retentionConfigMap("fugue-route-bundle-record-edge-pool-a-invalid", now.Add(-48*time.Hour), authorityLabels(group), map[string]string{
		"fugue.pro/authority-kind": "route-bundle",
	}, map[string]string{"record.json": `{}`}, true))
	client := fake.NewSimpleClientset(objects...)
	pruner, _ := NewArtifactPruner(client, "fugue-system", ArtifactRetentionPolicy{MinimumAge: time.Hour, MinimumHistory: 8, MaximumDeletes: 10})
	if result, err := pruner.Prune(context.Background(), now); err == nil {
		t.Fatalf("invalid inventory was accepted: %+v", result)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), candidateName, metav1.GetOptions{}); err != nil {
		t.Fatalf("valid artifact was deleted before validation completed: %v", err)
	}
}

func retentionConfigMap(name string, created time.Time, baseLabels, extraLabels, data map[string]string, immutable bool) *corev1.ConfigMap {
	labels := map[string]string{}
	for key, value := range baseLabels {
		labels[key] = value
	}
	for key, value := range extraLabels {
		labels[key] = value
	}
	value := immutable
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "fugue-system", UID: types.UID("uid-" + name), ResourceVersion: fmt.Sprintf("%d", created.Unix()),
			CreationTimestamp: metav1.NewTime(created), Labels: labels,
		},
		Immutable: &value,
		Data:      data,
	}
}

func TestArtifactRetentionReferenceClosureIsOrderIndependent(t *testing.T) {
	keep := map[string]bool{"execution": true}
	refs := map[string][]string{
		"execution": {"candidate"}, "candidate": {"rollback", "monitor"},
		"rollback": {"ancestor", "old-monitor"}, "ancestor": {"rollback"},
		"unreachable": {"unreachable-monitor"},
	}
	protectArtifactClosure(keep, refs)
	for _, name := range []string{"candidate", "rollback", "monitor", "ancestor", "old-monitor"} {
		if !keep[name] {
			t.Fatalf("lost transitive recovery dependency %s", name)
		}
	}
	if keep["unreachable"] || keep["unreachable-monitor"] {
		t.Fatal("unreferenced history was retained")
	}
}

func TestArtifactPrunerDefersWhenPublicationChangesDuringInventory(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	group := "edge-pool-a"
	objects := []runtime.Object{}
	for i := 0; i < 10; i++ {
		record := sealedRouteRecord(t, group, int64(i+1))
		raw, _ := declarativerelease.CanonicalJSON(record)
		objects = append(objects, retentionConfigMap(routeBundleRecordName(group, record.RecordDigest), now.Add(time.Duration(i-48)*time.Hour), authorityLabels(group), map[string]string{"fugue.pro/authority-kind": "route-bundle"}, map[string]string{"record.json": string(raw)}, true))
	}
	client := fake.NewSimpleClientset(objects...)
	lists := 0
	client.PrependReactor("list", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
		lists++
		if lists == 2 {
			cm := retentionConfigMap("fugue-candidate-authority-edge-pool-a", now, authorityLabels(group), nil, map[string]string{"candidate.json": "new publication"}, false)
			if err := client.Tracker().Add(cm); err != nil {
				t.Fatal(err)
			}
		}
		return false, nil, nil
	})
	pruner, _ := NewArtifactPruner(client, "fugue-system", ArtifactRetentionPolicy{MinimumAge: time.Hour, MinimumHistory: 8, MaximumDeletes: 2})
	result, err := pruner.Prune(context.Background(), now)
	if err != nil || !result.Deferred || result.Deleted != 0 {
		t.Fatalf("published reference changed during prune: %+v %v", result, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("deleted after a concurrent publication")
		}
	}
}

func TestArtifactPrunerAllowsHealthRefreshButFencesReferenceChanges(t *testing.T) {
	for _, changeReference := range []bool{false, true} {
		t.Run(fmt.Sprintf("reference-change=%t", changeReference), func(t *testing.T) {
			now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
			objects := []runtime.Object{}
			for i := 0; i < 10; i++ {
				record := sealedRouteRecord(t, "edge-pool-a", int64(i+1))
				raw, _ := declarativerelease.CanonicalJSON(record)
				objects = append(objects, retentionConfigMap(routeBundleRecordName(record.GroupID, record.RecordDigest), now.Add(time.Duration(i-48)*time.Hour), authorityLabels(record.GroupID), map[string]string{"fugue.pro/authority-kind": "route-bundle"}, map[string]string{"record.json": string(raw)}, true))
			}
			key := Key{Component: "api", Group: "global"}
			record, err := NewReleaseRecord(key, testSHA, testDigest, testDigest, otherDigest, testDigest)
			if err != nil {
				t.Fatal(err)
			}
			canary, err := NewCanaryResult(record, HealthHealthy, testDigest, now, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := declarativerelease.CanonicalJSON(canary)
			canaryObject := retentionConfigMap(canaryName(key), now, guardianLabels(key), nil, map[string]string{"result.json": string(raw)}, false)
			objects = append(objects, canaryObject)
			data, monitor, _, _ := guardianStableFixture(t, key, now)
			monitorName := monitorRecordNameFromDigest(key.Component, monitor.RecordDigest)
			objects = append(objects, retentionConfigMap(monitorName, now, map[string]string{"app.kubernetes.io/managed-by": "fugue-declarative-release", "fugue.pro/component": key.Component, "fugue.pro/config-sha": monitor.ConfigSHA}, nil, data, true))
			state, _, err := declarativerelease.NewMonitorState(monitor, declarativerelease.MonitorState{}, true, "", now)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ = declarativerelease.CanonicalJSON(state)
			monitorObject := retentionConfigMap("fugue-release-monitor-"+key.Component, now, map[string]string{"app.kubernetes.io/managed-by": "fugue-declarative-release"}, nil, map[string]string{"recordName": monitorName, "state.json": string(raw)}, false)
			objects = append(objects, monitorObject)
			client := fake.NewSimpleClientset(objects...)
			lists := 0
			client.PrependReactor("list", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
				lists++
				if lists == 2 {
					if changeReference {
						record.RecordDigest = otherDigest
					}
					canary, err = NewCanaryResult(record, HealthHealthy, testDigest, now.Add(time.Second), now.Add(2*time.Minute))
					if err != nil {
						t.Fatal(err)
					}
					raw, _ = declarativerelease.CanonicalJSON(canary)
					canaryObject.Data["result.json"] = string(raw)
					state.LastCheckedAt = now.Add(time.Second).Format(time.RFC3339Nano)
					state.LastHealthyAt = state.LastCheckedAt
					raw, _ = declarativerelease.CanonicalJSON(state)
					monitorObject.Data["state.json"] = string(raw)
					for _, object := range []*corev1.ConfigMap{canaryObject, monitorObject} {
						if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), object, "fugue-system"); err != nil {
							t.Fatal(err)
						}
					}
				}
				return false, nil, nil
			})
			pruner, _ := NewArtifactPruner(client, "fugue-system", ArtifactRetentionPolicy{MinimumAge: time.Hour, MinimumHistory: 8, MaximumDeletes: 2})
			result, err := pruner.Prune(context.Background(), now)
			if err != nil || result.Deferred != changeReference || (!changeReference && result.Deleted != 2) || (changeReference && result.Deleted != 0) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}
