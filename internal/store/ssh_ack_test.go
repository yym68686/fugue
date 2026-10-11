package store

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeSSHAcknowledgementPersistence(test *testing.T) {
	stateStore, _, _ := setupSSHIdentityStore(test)
	verifyEdgeSSHAcknowledgementStore(test, stateStore)
}

func verifyEdgeSSHAcknowledgementStore(test *testing.T, stateStore *Store) {
	test.Helper()
	now := time.Now().UTC()
	ack := model.EdgeSSHAcknowledgement{EdgeSSHAcknowledgementRequest: model.EdgeSSHAcknowledgementRequest{EdgeID: model.NewID("edge-ssh"), EdgeGroupID: "group-a", InstanceUID: "pod-a", BundleVersion: "bundle-a", RouteDigest: model.SSHRoutesDigest(nil), Persisted: true, LoadedAt: now, Connections: []model.EdgeSSHRouteConnections{}}, Routes: []model.EdgeSSHRoute{}, ObservedAt: now, ExpiresAt: now.Add(time.Minute)}
	if stateStore.usingDatabase() {
		defer stateStore.db.Exec(`DELETE FROM fugue_edge_ssh_acknowledgements WHERE edge_id=$1`, ack.EdgeID)
	}
	if err := stateStore.RecordEdgeSSHAcknowledgement(ack); err != nil {
		test.Fatal(err)
	}
	stored, err := stateStore.GetEdgeSSHAcknowledgement(ack.EdgeID, ack.InstanceUID)
	if err != nil || stored.RouteDigest != ack.RouteDigest || !stored.LoadedAt.Equal(now) {
		test.Fatal("ack did not roundtrip", err)
	}
	if _, err := stateStore.GetEdgeSSHAcknowledgement(ack.EdgeID, "another-instance"); !errors.Is(err, ErrNotFound) {
		test.Fatal("ack crossed instance boundary", err)
	}
	if err := stateStore.RecordEdgeSSHAcknowledgement(ack); !errors.Is(err, ErrConflict) {
		test.Fatal("old ACK refreshed its expiry", err)
	}
}
