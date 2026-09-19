// Package trafficbinding validates immutable provenance transported through the
// existing group authority path. Callers verify signatures before trusting it.
package trafficbinding

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fugue/internal/model"
	"regexp"
	"strings"
)

const Schema = "fugue.traffic-release-binding/v1"

var digest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var group = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)
var cohort = regexp.MustCompile(`^cohort=[a-z0-9]+(?:-[a-z0-9]+)*$`)

func Clone(in *model.TrafficReleaseBinding) *model.TrafficReleaseBinding {
	if in == nil {
		return nil
	}
	out := *in
	out.EdgeGroupIDs = append([]string(nil), in.EdgeGroupIDs...)
	return &out
}
func Validate(b *model.TrafficReleaseBinding) error {
	if b == nil {
		return nil
	}
	if b.Schema != Schema || b.RouteArtifactSequence <= 0 || b.FencingToken <= 0 {
		return errors.New("traffic binding schema or sequence invalid")
	}
	for _, s := range []string{b.ReleaseSetID, b.ReleaseSetGeneration, b.RouteArtifactID, b.RouteArtifactGeneration, b.ReleaseID, b.ScopeKey, b.CompilerVersion} {
		if s == "" || len(s) > 256 || strings.TrimSpace(s) != s || strings.ContainsAny(s, "\r\n\x00\t") {
			return errors.New("traffic binding identity invalid")
		}
	}
	for _, s := range []string{b.ReleaseSetDigest, b.RouteArtifactDigest, b.IntentDigest, b.PolicyDigest, b.InputSnapshotDigest, b.ProjectionDigest} {
		if !digest.MatchString(s) {
			return errors.New("traffic binding digest invalid")
		}
	}
	switch b.ReleaseChannel {
	case model.PlatformArtifactReleaseChannelShadow, model.PlatformArtifactReleaseChannelFull:
		if b.CanaryRuleRef != "" || len(b.EdgeGroupIDs) != 0 {
			return errors.New("non-canary traffic binding contains canary selection")
		}
	case model.PlatformArtifactReleaseChannelGray:
		if len(b.CanaryRuleRef) > 135 || !cohort.MatchString(b.CanaryRuleRef) || len(b.EdgeGroupIDs) == 0 || len(b.EdgeGroupIDs) > 64 {
			return errors.New("traffic binding canary missing")
		}
		prior := ""
		for _, g := range b.EdgeGroupIDs {
			if len(g) > 128 || !group.MatchString(g) || g <= prior {
				return errors.New("traffic binding groups invalid or noncanonical")
			}
			prior = g
		}
	default:
		return errors.New("traffic binding channel invalid")
	}
	return nil
}
func ProjectionDigest(snapshot model.EdgeRouteIntentSnapshot) string {
	snapshot.TrafficRelease = nil
	raw, _ := json.Marshal(snapshot)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func ValidateProjection(snapshot model.EdgeRouteIntentSnapshot) error {
	b := snapshot.TrafficRelease
	if b == nil {
		return nil
	}
	if err := Validate(b); err != nil {
		return err
	}
	if b.RouteArtifactGeneration != snapshot.Generation || b.ProjectionDigest != ProjectionDigest(snapshot) {
		return errors.New("traffic binding does not match route projection")
	}
	return nil
}
func ValidateGroup(b *model.TrafficReleaseBinding, id string, serving bool) error {
	if b == nil {
		return nil
	}
	if err := Validate(b); err != nil {
		return err
	}
	if !group.MatchString(id) {
		return errors.New("traffic binding group invalid")
	}
	if serving && b.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
		return errors.New("shadow traffic binding cannot serve")
	}
	if b.ReleaseChannel == model.PlatformArtifactReleaseChannelGray {
		for _, g := range b.EdgeGroupIDs {
			if g == id {
				return nil
			}
		}
		return errors.New("group is outside traffic binding canary")
	}
	return nil
}

// Once a group serves a ReleaseSet, absence of a release cannot re-enable a
// mutable configuration source. Rollback is a new fenced release of the old
// artifact; artifact sequence alone must not forbid that recovery.
func ValidateTransition(current, next *model.TrafficReleaseBinding) error {
	if err := Validate(next); err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	if next == nil {
		return errors.New("traffic authority cannot downgrade to an unbound source")
	}
	if err := Validate(current); err != nil {
		return err
	}
	if current.ScopeKey != next.ScopeKey {
		return errors.New("traffic authority scope changed")
	}
	if current.ReleaseChannel == next.ReleaseChannel {
		if next.FencingToken < current.FencingToken {
			return errors.New("traffic authority fence moved backwards")
		}
		if next.FencingToken == current.FencingToken {
			a, _ := json.Marshal(current)
			b, _ := json.Marshal(next)
			if string(a) != string(b) {
				return errors.New("traffic authority reused a fence for different provenance")
			}
		}
	}
	return nil
}
