package platformproducer

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"testing"
	"time"
)

func TestStaticIntentRejectsLossyOrDynamicMigration(t *testing.T) {
	for _, scenario := range []string{"valid", "disabled", "path", "upstreams", "owner", "cache", "tls", "dns consumers", "dynamic dns", "bad scope", "bad generation", "unknown field"} {
		t.Run(scenario, func(t *testing.T) {
			i := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "static", Routes: []platformconfig.RouteIntent{{Hostname: "static.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}, DNS: []platformconfig.DNSIntent{{Hostname: "record.example.test", Type: "TXT", Values: []string{"proof"}, TTL: 60, ValueExpirations: map[string]time.Time{"proof": time.Now().UTC().Add(time.Hour)}}}}
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, ScopeKey: "global", Generation: i.Generation}
			switch scenario {
			case "disabled":
				i.Routes[0].Enabled = false
			case "path":
				i.Routes[0].PathPrefix = "/api"
			case "upstreams":
				i.Routes[0].Upstreams = []platformconfig.UpstreamIntent{{}}
			case "owner":
				i.Routes[0].AppID = "business-app"
			case "cache":
				i.Routes[0].CacheNamespace = "cache"
			case "tls":
				i.TLS = []platformconfig.TLSIntent{{Hostname: "static.example.test", Policy: "platform"}}
			case "dns consumers":
				i.DNSConsumers = []platformconfig.DNSConsumerIntent{{NodeID: "dns"}}
			case "dynamic dns":
				i.DNS[0].Type = "FUGUE_APP"
			case "bad scope":
				a.ScopeKey = "other"
			case "bad generation":
				a.Generation = "other"
			}
			raw, _ := json.Marshal(i)
			json.Unmarshal(raw, &a.Content)
			if scenario == "unknown field" {
				a.Content["shell"] = "unknown"
			}
			out, err := DecodeStaticIntent(a)
			if scenario == "valid" || scenario == "disabled" {
				if err != nil || len(out.Routes) != 1 || len(out.DNS) != 1 || len(out.DNS[0].ValueExpirations) != 1 {
					t.Fatal("supported input rejected/lost", err)
				}
				if scenario == "disabled" && out.Routes[0].Status != model.EdgeRouteStatusDisabled {
					t.Fatal("disabled intent reenabled")
				}
			} else if err == nil {
				t.Fatal("lossy static input accepted")
			}
		})
	}
}
