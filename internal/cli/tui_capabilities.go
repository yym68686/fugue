package cli

import (
	"net/http"
	"time"
)

// The client must not silently send a new safety header to an old server that
// ignores it. Feature negotiation uses the server's executable OpenAPI contract.
func (p *tuiProvider) supportsActionReceipts(client *Client) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.actionCapabilitiesAt) < time.Minute {
		return p.actionReceipts
	}
	var spec struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
			Parameters  []struct {
				Name string `json:"name"`
				In   string `json:"in"`
			} `json:"parameters"`
		} `json:"paths"`
	}
	if err := client.doJSON(http.MethodGet, "/openapi.json", nil, &spec); err != nil {
		return false
	}
	supported := spec.Paths["/v1/apps/{id}/action-requests/{request_id}"]["get"].OperationID == "getAppActionRequest"
	for _, path := range []string{"/v1/apps/{id}/restart", "/v1/apps/{id}/scale", "/v1/apps/{id}/images/redeploy"} {
		idem, cas := false, false
		for _, parameter := range spec.Paths[path]["post"].Parameters {
			if parameter.In == "header" {
				idem = idem || parameter.Name == "Idempotency-Key"
				cas = cas || parameter.Name == "If-Match"
			}
		}
		supported = supported && idem && cas
	}
	p.actionReceipts, p.actionCapabilitiesAt = supported, time.Now()
	return supported
}
