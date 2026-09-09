package cli

import (
	"time"
)

// The server declares small feature flags alongside the principal. Missing
// flags on older servers keep TUI writes disabled; no full OpenAPI download.
func (p *tuiProvider) supportsActionReceipts(client *Client) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.actionCapabilitiesAt) < time.Minute {
		return p.actionReceipts
	}
	response, err := client.GetAuthContext()
	if err != nil {
		return false
	}
	p.actionReceipts, p.actionCapabilitiesAt = response.Capabilities.AppActionReceipts, time.Now()
	return p.actionReceipts
}
