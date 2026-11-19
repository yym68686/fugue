package platformcontrol

import (
	"regexp"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
)

var consumerNodeIDPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)
var workerEnrollmentPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

const workerEnrollmentSeparator = ":enrollment:"

func ValidWorkerEnrollmentID(value string) bool { return workerEnrollmentPattern.MatchString(value) }

// ConsumerAuthorityID identifies the new consumer namespace. A country or
// legacy group is never translated into a cell. Existing consumers retain
// their original IDs, cursors and evidence until an explicit cell is selected.
func ConsumerAuthorityID(group string) string {
	if strings.HasPrefix(group, "cell-") && edgetopology.ValidAuthorityID(group) {
		return group
	}
	return ""
}

// ConsumerScopeKey is the artifact scope selected by an explicit authority.
// Legacy groups retain the global compatibility scope until their credentials
// are deliberately enrolled in a cell.
func ConsumerScopeKey(group string) string {
	if authority := ConsumerAuthorityID(group); authority != "" {
		return "authority-cell:" + authority
	}
	return "global"
}

func ConfiguredConsumerScope(scope string) string {
	if scope == "" {
		return "global"
	}
	return scope
}

func ValidateConsumerScope(scope, group string) error {
	scope = ConfiguredConsumerScope(scope)
	if scope == "global" || ConsumerAuthorityID(group) != "" && scope == ConsumerScopeKey(group) {
		return nil
	}
	return ErrPlatformComponentIdentityInvalid
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

// EnrolledPlatformConsumerID keeps the physical Node and legacy group intact
// while giving a replacement global Worker its own lifecycle and cursor. The
// TLS sidecar uses the same enrollment when projected to its Worker owner.
func EnrolledPlatformConsumerID(component, node, authority, scope, enrollment string) (string, error) {
	id, err := PlatformConsumerID(component, node, authority)
	if err != nil || enrollment == "" {
		return id, err
	}
	if authority != "" || scope != "global" || !ValidWorkerEnrollmentID(enrollment) ||
		!consumerNodeIDPattern.MatchString(node) ||
		(component != model.PlatformConsumerComponentEdgeWorker && component != model.PlatformConsumerComponentCaddyEdgeFront) {
		return "", ErrPlatformComponentIdentityInvalid
	}
	return id + workerEnrollmentSeparator + enrollment, nil
}

// ConsumerEnrollmentFromID checks the exact component, physical node and
// scope before decoding an enrollment. It never accepts another node's ID.
func ConsumerEnrollmentFromID(component, node, authority, scope, id string) (string, error) {
	base, err := PlatformConsumerID(component, node, authority)
	if err != nil {
		return "", err
	}
	if id == base {
		return "", nil
	}
	suffix, ok := strings.CutPrefix(id, base+workerEnrollmentSeparator)
	if !ok || suffix == "" {
		return "", ErrPlatformComponentIdentityInvalid
	}
	canonical, err := EnrolledPlatformConsumerID(component, node, authority, scope, suffix)
	if err != nil || canonical != id {
		return "", ErrPlatformComponentIdentityInvalid
	}
	return suffix, nil
}

func (claims PlatformComponentIdentityClaims) ConsumerID() string {
	id, _ := EnrolledPlatformConsumerID(claims.Component, claims.NodeID, claims.AuthorityID, claims.ScopeKey, claims.WorkerEnrollmentID)
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
