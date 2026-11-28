package storageaudit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPolicyCannotAuthorizeDeletionOrWeakenProtection(t *testing.T) {
	p := Policy{Version: 1, Mode: "audit", EffectiveFrom: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		IntervalSeconds: 300, SourcePageRows: 2, HistoryDays: 7, MinimumVerifiedVersions: 3,
		UnusedUploadHours: 24, UnreferencedUploadDays: 30, WarningFreePercent: 20, PressureFreePercent: 15, CriticalFreePercent: 10}
	raw, _ := json.Marshal(p)
	if _, err := ParsePolicy(raw); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Policy){
		func(p *Policy) { p.Mode = "enforce" },
		func(p *Policy) { p.Mode = "" },
		func(p *Policy) { p.Version = 2 },
		func(p *Policy) { p.EffectiveFrom = time.Time{} },
		func(p *Policy) { p.IntervalSeconds = 299 },
		func(p *Policy) { p.SourcePageRows = 129 },
		func(p *Policy) { p.HistoryDays = 6 },
		func(p *Policy) { p.MinimumVerifiedVersions = 2 },
		func(p *Policy) { p.UnusedUploadHours = 23 },
		func(p *Policy) { p.UnreferencedUploadDays = 29 },
		func(p *Policy) { p.CriticalFreePercent = 15 },
		func(p *Policy) { p.WarningFreePercent = 51 },
	} {
		changed := p
		change(&changed)
		changedJSON, _ := json.Marshal(changed)
		if _, err := ParsePolicy(changedJSON); err == nil {
			t.Fatalf("unsafe policy accepted: %s", changedJSON)
		}
	}
	for _, extra := range []string{`,"tables":["fugue_apps"]}`, `,"delete_unknown":true}`, `} {}`} {
		if _, err := ParsePolicy(append(append([]byte{}, raw[:len(raw)-1]...), []byte(extra)...)); err == nil {
			t.Fatal("unknown fields or trailing document accepted")
		}
	}
	var production struct {
		Lifecycle Policy `json:"storageLifecycle"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "environments", "production", "storage-policy", "policy.json"))
	if err != nil || json.Unmarshal(raw, &production) != nil {
		t.Fatal("production policy unavailable", err)
	}
	raw, _ = json.Marshal(production.Lifecycle)
	if _, err = ParsePolicy(raw); err != nil || production.Lifecycle.Mode != "audit" {
		t.Fatal("production must start audit-only", err)
	}
}
