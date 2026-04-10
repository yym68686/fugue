package api

import (
	"strings"

	"fugue/internal/model"
)

func principalVisibilityCacheKey(principal model.Principal) string {
	tenantID := strings.TrimSpace(principal.TenantID)
	if principal.IsPlatformAdmin() {
		if tenantID == "" {
			return "platform"
		}
		return "platform:" + tenantID
	}
	return "tenant:" + tenantID
}
