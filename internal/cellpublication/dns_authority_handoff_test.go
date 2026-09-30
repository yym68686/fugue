package cellpublication

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
	"fugue/internal/testfixture/celldns"
)

func TestDNSAuthorityHandoffAuthenticatesBothSourcesAndEmbeddedMembership(t *testing.T) {
	for _, scenario := range []string{"complete", "forged old DNS", "forged new DNS", "forged embedded TLS", "foreign old artifact"} {
		t.Run(scenario, func(t *testing.T) {
			r := celldns.TransitionRequest(t)
			c := celldns.Compile(t, r)
			previous, next := r.PreviousTrafficPublication.DNS, c.DNSArtifact
			switch scenario {
			case "forged old DNS":
				previous.Provenance.Signature = "forged"
			case "forged new DNS":
				next.Provenance.Signature = "forged"
			case "foreign old artifact":
				previous = celldns.Sign(t, previous, "other-dns", 1)
			case "forged embedded TLS":
				raw, _ := json.Marshal(next)
				var copy model.PlatformArtifact
				json.Unmarshal(raw, &copy)
				next = copy
				next.Content["cell_route_publications"].([]any)[0].(map[string]any)["tls"].(map[string]any)["provenance"].(map[string]any)["signature"] = "forged"
				next = celldns.Sign(t, next, next.ID, 1)
			}
			err := VerifyDNSAuthorityHandoff(previous, next, "dns-a", celldns.Keys())
			if (err == nil) != (scenario == "complete") {
				t.Fatal("unexpected handoff integrity result", err)
			}
		})
	}
}
