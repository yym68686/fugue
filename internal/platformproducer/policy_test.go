package platformproducer

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
)

func TestPolicyRequiresBoundedTypedShadowControl(t *testing.T) {
	for _, scenario := range []string{"valid", "paused", "scope", "generation", "schema", "source", "target", "mode", "interval", "refresh", "short refresh", "unknown action"} {
		t.Run(scenario, func(t *testing.T) {
			p := Policy{SchemaVersion: Schema, Generation: "policy", Mode: "shadow", InputSource: "business-migration", TargetScope: "global", IntervalSeconds: 60, RefreshSeconds: 300}
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: Scope, Generation: p.Generation}
			switch scenario {
			case "paused":
				p.Mode = "paused"
			case "scope":
				a.ScopeKey = "global"
			case "generation":
				p.Generation = "other"
			case "schema":
				p.SchemaVersion = "unknown"
			case "source":
				p.InputSource = "shell"
			case "target":
				p.TargetScope = "other"
			case "mode":
				p.Mode = "full"
			case "interval":
				p.IntervalSeconds = 1
			case "refresh":
				p.RefreshSeconds = 3601
			case "short refresh":
				p.IntervalSeconds = 600
				p.RefreshSeconds = 300
			}
			raw, _ := json.Marshal(p)
			json.Unmarshal(raw, &a.Content)
			if scenario == "unknown action" {
				a.Content["command"] = "arbitrary action"
			}
			_, err := Decode(a)
			if (scenario == "valid" || scenario == "paused") != (err == nil) {
				t.Fatal("policy admission differs", err)
			}
		})
	}
}
