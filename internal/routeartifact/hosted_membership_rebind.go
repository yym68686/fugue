package routeartifact

import (
	"errors"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// A new, signed membership binding, not an extension of the old command's
// validity or its observations. Predecessor names the entire old per-member
// packet, including its scope, enrollment, parent, phase and physical parts.
// Baseline pins the new parent's actual host rows and certificate owner, or
// its complete set of DNS views. It is not the digest of proposed rows.
type HostedMembershipRebind struct {
	Predecessor string `json:"predecessor_packet"`
	Baseline    string `json:"host_baseline"`
}

// The producer and receiver run the same comparison over independently
// resolved images. Computing this digest does not issue a new decision.
func HostedContinuationPredecessor(old HostedSelectedPacket, next HostedDelivery, baseline string) (string, error) {
	if !platformproducer.ValidDigest(baseline) || old.MemberBaseline != baseline || old.Delivery.Host == "" || old.Delivery.Host != next.Host ||
		old.Delivery.Member == "" || old.Delivery.Member != next.Member || old.Delivery.Phase != next.Phase ||
		(next.Phase != "forward" && next.Phase != "prepare") || old.Delivery.DraftDigest == next.DraftDigest ||
		old.Delivery.ImageDigest == next.ImageDigest || old.Delivery.MembershipFence >= next.MembershipFence ||
		old.Delivery.MembershipRelease == next.MembershipRelease || len(old.Delivery.Parts) == 0 || len(old.Delivery.Parts) != len(next.Parts) || len(next.Parts) > 4096 {
		return "", errors.New("different host, membership, phase or baseline")
	}
	for i, b := range next.Parts {
		a := old.Delivery.Parts[i]
		if a.Recipient != b.Recipient || !platformproducer.ValidDigest(a.ContentDigest) || a.ContentDigest != b.ContentDigest {
			return "", errors.New("different member content")
		}
	}
	return pc.Digest([]any{"fugue.host-member-predecessor/v1", old})
}

// Called with the two independently compiled and verified member commands;
// this operation cannot change the phase, normalized routes, owner or target
// identity. A real edit/withdrawal is a separate decision on the new parent.
// A signer must not infer either baseline from the historical packet.
func BindHostedContinuation(old, next HostedSelectedPacket, oldBaseline, newBaseline string) (*HostedMembershipRebind, error) {
	bad := errors.New("host continuation differs from its exact predecessor or baseline")
	if !platformproducer.ValidDigest(oldBaseline) || oldBaseline != newBaseline ||
		old.MemberBaseline != oldBaseline || next.MemberBaseline != newBaseline ||
		old.Scope == "" || old.Scope != next.Scope || old.Project == "" || old.Project != next.Project ||
		old.Delivery.Host != next.Delivery.Host || old.Delivery.Member != next.Delivery.Member ||
		old.Business != next.Business || old.Authority == next.Authority || old.Parent == next.Parent ||
		old.Draft == next.Draft || old.Delivery.DraftDigest == next.Delivery.DraftDigest ||
		old.Delivery.ImageDigest == next.Delivery.ImageDigest || next.Offer <= old.Offer ||
		next.Fence <= old.Fence || next.Fence-1 != old.Fence || next.Previous != old.Decision ||
		next.Decision == old.Decision || next.SelectedAt.Before(old.SelectedAt) ||
		old.Delivery.Phase != next.Delivery.Phase || (next.Delivery.Phase != "prepare" && next.Delivery.Phase != "forward") ||
		old.Delivery.MembershipFence >= next.Delivery.MembershipFence || old.Delivery.MembershipRelease == next.Delivery.MembershipRelease ||
		len(old.Delivery.Parts) == 0 || len(old.Delivery.Parts) != len(next.Delivery.Parts) || len(next.Delivery.Parts) > 4096 {
		return nil, bad
	}
	for i, b := range next.Delivery.Parts {
		a := old.Delivery.Parts[i]
		k, e := next.Delivery.PartKey(b)
		if e != nil || a.Recipient != b.Recipient || !platformproducer.ValidDigest(a.ContentDigest) ||
			a.ContentDigest != b.ContentDigest || next.PriorParts[k] != a.ContentDigest {
			return nil, bad
		}
	}
	pin, e := HostedContinuationPredecessor(old, next.Delivery, newBaseline)
	if e != nil {
		return nil, e
	}
	return &HostedMembershipRebind{pin, newBaseline}, nil
}

// Signed old/new global children are projected separately by the caller.
// All physical views, including delegated zones and their TTLs, must match.
// Deliberately exclude receipts, the DNS response cache and observation time.
func HostedDNSBaselinePin(d HostedDelivery, base map[HostedRecipient]HostedRecipientInput) (string, error) {
	if d.Host == "" || len(d.Parts) == 0 || len(d.Parts) > 4096 || len(base) != len(d.Parts) {
		return "", errors.New("incomplete DNS baseline")
	}
	items := make([]any, 0, len(base))
	last := ""
	for _, p := range d.Parts {
		r := p.Recipient
		v, ok := base[r]
		if !ok || r.Kind != model.PlatformArtifactKindDNSAnswerBundle || r.Zone <= last || v.DNS == nil {
			return "", errors.New("foreign or unordered DNS baseline")
		}
		items = append(items, []any{r, v})
		last = r.Zone
	}
	return pc.Digest([]any{"fugue.host-dns-baseline/v1", d.Host, items})
}

// Match a freshly verified packet against the last locally committed
// signed predecessor. A controller signature alone does not reset a host's
// fence on restart; the old packet's integrity remains a separate check.
func (p HostedSelectedPacket) ContinuesHostedMembership(old HostedSelectedPacket, oldBaseline, newBaseline string) bool {
	if p.MembershipRebind == nil {
		return false
	}
	r, e := BindHostedContinuation(old, p, oldBaseline, newBaseline)
	return e == nil && *r == *p.MembershipRebind
}
