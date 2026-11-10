package inputarchive

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	"fugue/internal/staticedgecontract"
)

// PodTokenHasExclusiveAudience is syntax/purpose preflight, NOT authentication.
// TokenReview must still verify this exact signed token and bound live identity.
// Status.Audiences only reports the intersection with requested audiences, so
// it cannot prove that the original token has no other audience. Reject duplicate
// claims and multiple audiences in the original payload before requesting review.
func PodTokenHasExclusiveAudience(token, audience string) bool {
	parts := strings.Split(token, ".")
	if audience == "" || len(token) > 32768 || len(parts) != 3 || parts[0] == "" || parts[2] == "" || strings.ContainsAny(token, " \t\r\n") {
		return false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil {
		return false
	}
	if _, err := staticedgecontract.CanonicalJSON(raw); err != nil {
		return false
	}
	// JWT claim names are case-sensitive. A struct would accept a later "Aud"
	// and could hide a multi-audience "aud" from this preflight while the
	// authoritative reviewer correctly interprets the original lower-case claim.
	var claims map[string]json.RawMessage
	var audiences []string
	return json.Unmarshal(raw, &claims) == nil && json.Unmarshal(claims["aud"], &audiences) == nil && len(audiences) == 1 && audiences[0] == audience
}
