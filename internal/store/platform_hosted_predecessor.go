package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"fugue/internal/platformproducer"
	"fugue/internal/routeartifact"
)

type hostedRebindJournal struct {
	hostedSelectionHead
	packets map[string]string
}

func pgHostedRebindJournal(ctx context.Context, tx *sql.Tx, host string, enabled bool) (j hostedRebindJournal, err error) {
	if !enabled {
		return j, nil
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT fence,decision,phase,predecessor_packets FROM fugue_hosted_membership_rebinds WHERE hostname=$1`, host).Scan(&j.Fence, &j.Decision, &j.Phase, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return j, nil
	}
	if err != nil {
		return j, err
	}
	if json.Unmarshal(raw, &j.packets) != nil || len(j.packets) == 0 || len(j.packets) > 8192 {
		return j, ErrConflict
	}
	return j, nil
}

func hostedSelectionManifests(manifest, previous []byte) (map[string]string, map[string]string, error) {
	var members, prior map[string]string
	if json.Unmarshal(manifest, &members) != nil || len(members) == 0 || len(members) > 8192 ||
		json.Unmarshal(previous, &prior) != nil || prior == nil || len(prior) > 8192 {
		return nil, nil, ErrConflict
	}
	return members, prior, nil
}

// Shared by live delivery and by reconstruction of the previously issued
// command. The rollback image on a rebind is *not* the previous live phase.
func hostedWirePredecessor(p routeartifact.HostedSelectedPacket, members, prior map[string]string, j hostedRebindJournal) (routeartifact.HostedSelectedPacket, error) {
	var zero routeartifact.HostedSelectedPacket
	if j.Fence > p.Fence {
		return zero, ErrConflict
	}
	carry := j.Fence > 0 && j.Fence == p.Fence
	if carry && (j.Decision != p.Decision || j.Phase != p.Delivery.Phase || !platformproducer.ValidDigest(p.MemberBaseline) || len(j.packets) != len(members)) {
		return zero, ErrConflict
	}
	if j.Fence > 0 && j.Fence == p.Fence-1 && p.Delivery.Phase == "withdraw" {
		if j.Phase != "forward" || j.Decision != p.Previous {
			return zero, ErrConflict
		}
		p.SkippedForwardFrom = ""
		p.ExactWithdrawal = true
	}
	p.PriorParts = map[string]string{}
	for _, part := range p.Delivery.Parts {
		k, e := p.Delivery.PartKey(part)
		if e != nil || members[k] != part.ContentDigest {
			return zero, ErrConflict
		}
		if p.Fence == 1 {
			continue
		}
		expected := prior[k]
		if carry {
			pin := j.packets[k]
			if !platformproducer.ValidDigest(pin) || p.MembershipRebind != nil && p.MembershipRebind.Predecessor != pin {
				return zero, ErrConflict
			}
			p.MembershipRebind = &routeartifact.HostedMembershipRebind{Predecessor: pin, Baseline: p.MemberBaseline}
			expected = members[k]
		}
		if !platformproducer.ValidDigest(expected) {
			return zero, ErrConflict
		}
		p.PriorParts[k] = expected
	}
	// These fields form part of the full predecessor digest. PostgreSQL stores
	// instants, not the API replica's local timezone.
	if p.MemberBaseline != "" {
		p.SelectedAt = p.SelectedAt.UTC()
		p.ValidUntil = p.ValidUntil.UTC()
	}
	return p, nil
}
