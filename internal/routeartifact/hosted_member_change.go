package routeartifact

import (
	"errors"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

// Durable high water for a single host, enrollment and artifact kind.
// This identifies a local configuration, not that a DNS or route probe passed.
type HostedMemberCursor struct {
	Host, Member, Kind, Decision string
	Fence                        int64
}

// A complete per-process compare-and-exchange (including every declared DNS
// zone). Compute it under the executor's snapshot lock; publish the result
// and cursor atomically, or leave both untouched. Needed contains only the
// slots which actually need different bytes. No expiry extension or ACK.
type HostedMemberChange struct {
	Next   HostedMemberCursor
	Needed []HostedDeliveryPart
}

// Live is projected from what the executor is actually using. Base is
// independently derived from the exact verified serving parent, never
// populated from the new command; it is needed only for first admission.
func PlanHostedMemberChange(a model.PlatformArtifact, host string, who platformcontrol.PlatformComponentIdentityClaims, serving model.PlatformConsumerAssignment, cursor HostedMemberCursor, live, base map[HostedRecipient]HostedRecipientInput, keys bundleauth.Keyring, now time.Time) (HostedMemberChange, error) {
	var zero HostedMemberChange
	bad := errors.New("local host or recipient differs from the selected predecessor")
	p, err := VerifyHostedPacket(a, host, who, serving, cursor.Fence, cursor.Decision, keys, now)
	if err != nil {
		return zero, err
	}
	d := p.Delivery
	next := HostedMemberCursor{host, d.Member, serving.ArtifactKind, p.Decision, p.Fence}
	if (cursor.Fence == 0 && cursor != (HostedMemberCursor{})) ||
		(cursor.Fence != 0 && (cursor.Host != host || cursor.Member != next.Member || cursor.Kind != next.Kind)) ||
		len(live) != len(d.Parts) {
		return zero, bad
	}
	initial := p.Fence == 1 && cursor.Fence == 0
	repeat := cursor == next
	skipped := p.Delivery.Phase == "withdraw" && !p.ExactWithdrawal && cursor.Fence == p.Fence-2 && cursor.Decision == p.SkippedForwardFrom
	if !initial && !repeat && !skipped && (cursor.Fence != p.Fence-1 || cursor.Decision != p.Previous) {
		return zero, bad
	}
	if initial && len(base) != len(d.Parts) {
		return zero, bad
	}
	change := HostedMemberChange{Next: next}
	for _, part := range d.Parts {
		r := part.Recipient
		l, ok := live[r]
		if !ok {
			return zero, bad
		}
		digest := func(v HostedRecipientInput) (string, error) {
			return pc.Digest([]any{"fugue.hosted-recipient-part/v1", host, r, v})
		}
		observed, e := digest(l)
		if e != nil {
			return zero, e
		}
		k, e := d.PartKey(part)
		if e != nil {
			return zero, e
		}
		expected := p.PriorParts[k]
		switch {
		case initial:
			original, ok := base[r]
			if !ok {
				return zero, bad
			}
			expected, e = digest(original)
			if e != nil {
				return zero, e
			}
		case repeat, skipped:
			expected = part.ContentDigest
		}
		if observed != expected {
			return zero, bad
		}
		if observed != part.ContentDigest {
			change.Needed = append(change.Needed, part)
		}
	}
	return change, nil
}
