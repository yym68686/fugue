package platformcontrol

import (
	"regexp"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
)

var consumerNodeIDPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)

// ConsumerAuthorityID identifies the new consumer namespace. A country or
// legacy group is never translated into a cell. Existing consumers retain
// their original IDs, cursors and evidence until an explicit cell is selected.
func ConsumerAuthorityID(group string) string {
	if strings.HasPrefix(group, "cell-") && edgetopology.ValidAuthorityID(group) {
		return group
	}
	return ""
}

// PlatformConsumerID keeps the physical Edge identity independent from the
// authority whose artifact it observes. It grants no assignment or traffic.
func PlatformConsumerID(component, node, authority string) (string, error) {
	if authority == "" {
		return component + ":" + node, nil
	}
	if ConsumerAuthorityID(authority) != authority || !consumerNodeIDPattern.MatchString(node) ||
		(component != model.PlatformConsumerComponentEdgeWorker && component != model.PlatformConsumerComponentCaddyEdgeFront && component != model.PlatformConsumerComponentEdgeControl && component != model.PlatformConsumerComponentDNSServer) {
		return "", ErrPlatformComponentIdentityInvalid
	}
	return component + ":" + authority + ":" + node, nil
}

func (claims PlatformComponentIdentityClaims) ConsumerID() string {
	id, _ := PlatformConsumerID(claims.Component, claims.NodeID, claims.AuthorityID)
	return id
}

// ExpectedConsumerIdentityMatches compares explicit scope as well as its
// encoded ID. A familiar node name alone never carries evidence into a cell.
func ExpectedConsumerIdentityMatches(expected model.PlatformExpectedConsumer, claims PlatformComponentIdentityClaims) bool {
	id := claims.ConsumerID()
	scopedOwner := claims.Component == model.PlatformConsumerComponentEdgeWorker || claims.Component == model.PlatformConsumerComponentCaddyEdgeFront || claims.Component == model.PlatformConsumerComponentDNSServer
	return id != "" && expected.ConsumerID == id && expected.Component == claims.Component &&
		expected.NodeID == claims.NodeID && expected.AuthorityID == claims.AuthorityID &&
		(!scopedOwner || ConsumerAuthorityID(expected.Cohort) == "" || expected.AuthorityID == expected.Cohort) &&
		(claims.AuthorityID == "" || expected.Cohort == claims.AuthorityID)
}
