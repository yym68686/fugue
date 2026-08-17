package store

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficoverride"
)

func TestTrafficOverrideStoreCASAndSigningKeyRotation(t *testing.T) {
	path := t.TempDir() + "/state.json"
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	initial, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(initial), "traffic_override_signing") {
		t.Fatal("store init must not create override signing state before the independent management path is used")
	}
	keyring, err := s.GetTrafficOverrideSigningKeyring()
	if err != nil || keyring.Generation != 1 || keyring.CurrentKeyID == "" || keyring.CurrentPrivateKey == "" || keyring.CurrentPublicKey == "" {
		t.Fatalf("unexpected initial keyring: %+v err=%v", keyring.Status(), err)
	}
	now := time.Now().UTC()
	candidate := model.TrafficOverride{
		Hostname:           "app.example.com",
		Generation:         1,
		State:              model.TrafficOverrideStateStaged,
		Answers:            []string{"192.0.2.10"},
		RequiredHostRoutes: []string{"app.example.com"},
		RouteGeneration:    "route-generation-1",
		RouteDigest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpiresAt:          now.Add(time.Hour),
		Reason:             "emergency route test",
		Operator:           "api-key/admin",
		SignedAt:           now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	candidate, err = trafficoverride.Sign(candidate, keyring.CurrentPrivateKey, keyring.CurrentKeyID)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := s.PutTrafficOverrideCAS(candidate, 0)
	if err != nil || stored.Generation != 1 {
		t.Fatalf("put override: %+v err=%v", stored, err)
	}
	if _, err := s.PutTrafficOverrideCAS(candidate, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale create CAS must conflict: %v", err)
	}
	rotated, err := s.RotateTrafficOverrideSigningKeyring(keyring.Generation)
	if err != nil || rotated.Generation != 2 || rotated.PreviousKeyID != keyring.CurrentKeyID || rotated.CurrentKeyID == keyring.CurrentKeyID {
		t.Fatalf("rotate keyring: %+v err=%v", rotated.Status(), err)
	}
	if err := trafficoverride.VerifyWithKeyring(stored, rotated); err != nil {
		t.Fatalf("stored artifact must verify with retained previous key: %v", err)
	}
}
