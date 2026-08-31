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
