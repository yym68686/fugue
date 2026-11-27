package platformcontrol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"strings"
	"time"

	"fugue/internal/model"
)

const participationObserverScope = "participation-observation:"

const ParticipationObserverTTL = 2 * time.Minute

var observationSubject = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,200}$`)

// DeriveParticipationObserverKeyring separates observation tokens from
// platform component credentials even if a deployment repeats a key value.
func DeriveParticipationObserverKeyring(secret, id string) PlatformComponentIdentityKeyring {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return PlatformComponentIdentityKeyring{}
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("fugue/participation-observer/v1"))
	derived := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return DerivePlatformComponentIdentityKeyring(derived, id, "", "", nil)
}

func ParticipationObserverClaims(node, group string) (PlatformComponentIdentityClaims, error) {
	if !observationSubject.MatchString(node) || !observationSubject.MatchString(group) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	return PlatformComponentIdentityClaims{
		Component: model.PlatformConsumerComponentNodeGuardian, CredentialID: "participation-shadow",
		NodeID: node, ScopeKey: participationObserverScope + group,
		ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle},
	}, nil
}

func ParticipationObserverGroup(claims PlatformComponentIdentityClaims, node string) (string, bool) {
	group, ok := strings.CutPrefix(claims.ScopeKey, participationObserverScope)
	if !ok || !observationSubject.MatchString(group) || !observationSubject.MatchString(node) || claims.NodeID != node ||
		claims.Component != model.PlatformConsumerComponentNodeGuardian || claims.CredentialID != "participation-shadow" ||
		claims.AuthorityID != "" || claims.WorkerEnrollmentID != "" || claims.WorkerFrontServiceUID != "" ||
		claims.IssuedAtUnix <= 0 || claims.ExpiresAtUnix <= claims.IssuedAtUnix ||
		claims.ExpiresAtUnix-claims.IssuedAtUnix > int64(ParticipationObserverTTL/time.Second) ||
		len(claims.ArtifactKinds) != 1 || claims.ArtifactKinds[0] != model.PlatformArtifactKindDNSAnswerBundle {
		return "", false
	}
	return group, true
}
