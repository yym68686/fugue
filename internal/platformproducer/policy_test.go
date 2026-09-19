package platformproducer

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestPolicyRequiresBoundedTypedShadowControl(t *testing.T) {
	for _, scenario := range []string{"valid", "domains", "domains without static", "paused", "static", "static missing id", "static bad digest", "migration with ref", "scope", "generation", "schema", "source", "target", "mode", "interval", "refresh", "short refresh", "unknown action", "dns", "dns missing digest", "dns without static", "template without policy", "duplicate template"} {
		t.Run(scenario, func(t *testing.T) {
			p := Policy{SchemaVersion: Schema, Generation: "policy", Mode: "shadow", InputSource: "business-migration", TargetScope: "global", IntervalSeconds: 60, RefreshSeconds: 300}
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: Scope, Generation: p.Generation}
			switch scenario {
			case "static", "domains", "static missing id", "static bad digest":
				p.RequireApplicationDomains = scenario == "domains"
				p.InputSource = "business-static-intent"
				p.StaticIntentArtifactID = "artifact-static"
				p.StaticIntentDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				if scenario == "static missing id" {
					p.StaticIntentArtifactID = ""
				}
				if scenario == "static bad digest" {
					p.StaticIntentDigest = "sha256:invalid"
				}
			case "domains without static":
				p.RequireApplicationDomains = true
			case "migration with ref":
				p.StaticIntentArtifactID = "ignored-ref"
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
			if strings.HasPrefix(scenario, "dns") || strings.Contains(scenario, "template") {
				p.InputSource = "business-static-intent"
				p.StaticIntentArtifactID = "base"
				p.StaticIntentDigest = "sha256:" + strings.Repeat("a", 64)
				p.DNSPolicyArtifactID = "dns"
				p.DNSPolicyDigest = "sha256:" + strings.Repeat("b", 64)
				p.HostedZoneTemplates = []HostedZoneTemplate{{NodeID: "dns-a", TemplateZone: "example.test"}}
				switch scenario {
				case "dns missing digest":
					p.DNSPolicyDigest = ""
				case "dns without static":
					p.InputSource = "business-migration"
					p.StaticIntentArtifactID = ""
					p.StaticIntentDigest = ""
				case "template without policy":
					p.DNSPolicyArtifactID = ""
					p.DNSPolicyDigest = ""
				case "duplicate template":
					p.HostedZoneTemplates = append(p.HostedZoneTemplates, p.HostedZoneTemplates[0])
				}
			}
			raw, _ := json.Marshal(p)
			json.Unmarshal(raw, &a.Content)
			if scenario == "unknown action" {
				a.Content["command"] = "arbitrary action"
			}
			_, err := Decode(a)
			if (scenario == "domains" || scenario == "valid" || scenario == "paused" || scenario == "static" || scenario == "dns") != (err == nil) {
				t.Fatal("policy admission differs", err)
			}
		})
	}
}
