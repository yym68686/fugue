package store

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDemoteLegacyImageLocationPreservesDigestQualifiedIdentity(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	imageRef := "registry.fugue.internal:5000/fugue-apps/demo@" + digest
	expectedUpdatedAt := time.Now().UTC().Add(-time.Minute)
	observedAt := expectedUpdatedAt.Add(30 * time.Second)
	legacy := model.ImageLocation{
		ID:                "legacy-location",
		TenantID:          "tenant",
		AppID:             "app",
		ImageRef:          imageRef,
		Digest:            "",
		SourceOperationID: "legacy-operation",
		NodeID:            "node",
		RuntimeID:         "runtime",
		ClusterNodeName:   "worker",
		CacheEndpoint:     "http://worker:5000",
		Status:            " PRESENT ",
		LastSeenAt:        &expectedUpdatedAt,
		SizeBytes:         123,
		CreatedAt:         expectedUpdatedAt.Add(-time.Hour),
		UpdatedAt:         expectedUpdatedAt,
	}
	canonical := legacy
	canonical.ID = "canonical-location"
	canonical.AppID = "canonical-app"
	canonical.Digest = digest
	canonical.Status = model.ImageLocationStatusPresent
	canonical.LastError = ""
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		state.ImageLocations = append(state.ImageLocations, legacy, canonical)
		return nil
	}); err != nil {
		t.Fatalf("seed image locations: %v", err)
	}
	refreshedUpdatedAt := expectedUpdatedAt.Add(time.Second)
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		for index := range state.ImageLocations {
			if state.ImageLocations[index].ID == legacy.ID {
				state.ImageLocations[index].UpdatedAt = refreshedUpdatedAt
				return nil
			}
		}
		return ErrNotFound
	}); err != nil {
		t.Fatalf("advance concurrent refresh fence: %v", err)
	}

	const reason = "superseded by canonical-digest image location"
	if _, err := stateStore.DemoteLegacyImageLocationIfUnchanged(
		legacy.ID,
		expectedUpdatedAt,
		observedAt,
		reason,
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("concurrent refresh compare-and-swap error = %v, want conflict", err)
	}
	var refreshed model.ImageLocation
	if err := stateStore.withLockedState(false, func(state *model.State) error {
		for _, location := range state.ImageLocations {
			if location.ID == legacy.ID {
				refreshed = location
				return nil
			}
		}
		return ErrNotFound
	}); err != nil {
		t.Fatalf("read concurrently refreshed location: %v", err)
	}
	if refreshed.Digest != "" ||
		!strings.EqualFold(strings.TrimSpace(refreshed.Status), model.ImageLocationStatusPresent) ||
		!refreshed.UpdatedAt.Equal(refreshedUpdatedAt) {
		t.Fatalf("concurrent refresh was changed by stale demotion: %+v", refreshed)
	}

	demoted, err := stateStore.DemoteLegacyImageLocationIfUnchanged(
		legacy.ID,
		refreshedUpdatedAt,
		observedAt,
		reason,
	)
	if err != nil {
		t.Fatalf("demote legacy location: %v", err)
	}
	if demoted.ID != legacy.ID || demoted.ImageRef != imageRef || demoted.Digest != "" ||
		demoted.Status != model.ImageLocationStatusMissing || demoted.LastError != reason {
		t.Fatalf("unexpected demoted location: %+v", demoted)
	}
	if demoted.LastSeenAt == nil || !demoted.LastSeenAt.Equal(observedAt) {
		t.Fatalf("demoted location observation = %v, want %v", demoted.LastSeenAt, observedAt)
	}
	if !demoted.UpdatedAt.After(refreshedUpdatedAt) {
		t.Fatalf("demoted location updated_at = %v, want after %v", demoted.UpdatedAt, refreshedUpdatedAt)
	}
	if demoted.SourceOperationID != legacy.SourceOperationID || demoted.NodeID != legacy.NodeID ||
		demoted.RuntimeID != legacy.RuntimeID || demoted.ClusterNodeName != legacy.ClusterNodeName ||
		demoted.CacheEndpoint != legacy.CacheEndpoint || demoted.SizeBytes != legacy.SizeBytes {
		t.Fatalf("demotion changed non-state fields: before=%+v after=%+v", legacy, demoted)
	}

	missingLocations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		ImageRef:      imageRef,
		Status:        model.ImageLocationStatusMissing,
		PlatformAdmin: true,
	})
	if err != nil {
		t.Fatalf("list missing image locations: %v", err)
	}
	if len(missingLocations) != 1 || missingLocations[0].ID != legacy.ID ||
		missingLocations[0].Digest != "" || missingLocations[0].Status != model.ImageLocationStatusMissing {
		t.Fatalf("legacy identity changed: %+v", missingLocations)
	}
	presentLocations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		ImageRef:      imageRef,
		Status:        model.ImageLocationStatusPresent,
		PlatformAdmin: true,
	})
	if err != nil {
		t.Fatalf("list present image locations: %v", err)
	}
	if len(presentLocations) != 1 || presentLocations[0].ID != canonical.ID ||
		presentLocations[0].AppID != canonical.AppID || presentLocations[0].Digest != digest ||
		presentLocations[0].Status != model.ImageLocationStatusPresent ||
		!presentLocations[0].UpdatedAt.Equal(expectedUpdatedAt) {
		t.Fatalf("canonical location changed: %+v", presentLocations)
	}
}

func TestDemoteLegacyImageLocationValidatesFenceAndLegacyState(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Now().UTC()
	for _, test := range []struct {
		name       string
		id         string
		expectedAt time.Time
		observedAt time.Time
		reason     string
	}{
		{name: "missing id", expectedAt: now, observedAt: now, reason: "reason"},
		{name: "missing expected time", id: "location", observedAt: now, reason: "reason"},
		{name: "missing observed time", id: "location", expectedAt: now, reason: "reason"},
		{name: "missing reason", id: "location", expectedAt: now, observedAt: now},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if _, err := stateStore.DemoteLegacyImageLocationIfUnchanged(
				test.id,
				test.expectedAt,
				test.observedAt,
				test.reason,
			); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("validation error = %v, want invalid input", err)
			}
		})
	}

	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, test := range []struct {
		name     string
		location model.ImageLocation
	}{
		{
			name: "canonical digest",
			location: model.ImageLocation{
				ID: "canonical", ImageRef: "registry.example/demo:tag", Digest: digest,
				NodeID: "node", Status: model.ImageLocationStatusPresent, UpdatedAt: now,
			},
		},
		{
			name: "already missing",
			location: model.ImageLocation{
				ID: "missing", ImageRef: "registry.example/demo:tag",
				NodeID: "node", Status: model.ImageLocationStatusMissing, UpdatedAt: now,
			},
		},
		{
			name: "not distributed",
			location: model.ImageLocation{
				ID: "local", ImageRef: "registry.example/demo:tag",
				Status: model.ImageLocationStatusPresent, UpdatedAt: now,
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if err := stateStore.withLockedState(true, func(state *model.State) error {
				state.ImageLocations = append(state.ImageLocations, test.location)
				return nil
			}); err != nil {
				t.Fatalf("seed ineligible location: %v", err)
			}
			if _, err := stateStore.DemoteLegacyImageLocationIfUnchanged(
				test.location.ID,
				now,
				now.Add(time.Second),
				"ineligible location",
			); !errors.Is(err, ErrConflict) {
				t.Fatalf("ineligible state error = %v, want conflict", err)
			}
		})
	}
}
