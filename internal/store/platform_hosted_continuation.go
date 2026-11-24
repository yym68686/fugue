package store

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"reflect"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/routeartifact"
)

type hostedDeliveryRecord struct {
	assignment model.PlatformConsumerAssignment
	delivery   routeartifact.HostedDelivery
}
type hostedDeliverySet map[[2]string]hostedDeliveryRecord

func (d hostedDeliverySet) add(a model.PlatformConsumerAssignment, v routeartifact.HostedDelivery) error {
	k := [2]string{v.Member, a.ArtifactKind}
	if _, found := d[k]; found || v.Member == "" || len(v.Parts) == 0 {
		return ErrConflict
	}
	d[k] = hostedDeliveryRecord{a, v}
	return nil
}

// A different global release may only carry this exact host's business
// program. Changing an owner, address hold, target, or route is not renewal.
func sameHostedProposalAcrossParents(old, p pc.HostedScopeProposal) bool {
	if !old.HasMemberBaselines() || !p.HasMemberBaselines() || old.Authority.Producer != p.Authority.Producer ||
		old.Base.FencingToken >= p.Base.FencingToken || old.Base.ReleaseSetID == p.Base.ReleaseSetID || old.Base.ReleaseID == p.Base.ReleaseID {
		return false
	}
	old.Base = p.Base
	old.Authority = p.Authority
	return reflect.DeepEqual(old, p)
}

// Called inside the admitted offer transaction. The current full/LKG and
// current member facts have been checked. Historical signatures reconstruct
// the predecessor; they confer no lease or serving rights on the new full.
func (s *Store) pgContinueHostedOffer(ctx context.Context, tx *sql.Tx, full model.PlatformArtifact, p pc.HostedScopeProposal, program routeartifact.CompiledHostedProgram, cmd hostedSelectionCommand, directory []hostedMemberDirectoryEntry, nextSet hostedDeliverySet, nextUnit *routeartifact.HostedPhaseResolver) error {
	c := cmd.Claim
	var old routeartifact.HostedSelectedPacket
	var phase, hash, image string
	var body, manifest, priorManifest []byte
	err := tx.QueryRowContext(ctx, `SELECT scope_key,project_id,offer_sequence,draft_id,draft_digest,program_digest,program,
 fence,decision,phase,members,authority_digest,business_digest,selected_at,valid_until,previous_decision,previous_members,skipped_forward_from
 FROM fugue_hosted_selections WHERE hostname=$1`, c.Host).Scan(&old.Scope, &old.Project, &old.Offer, &old.Draft, &hash, &image, &body, &old.Fence, &old.Decision, &phase, &manifest, &old.Authority, &old.Business, &old.SelectedAt, &old.ValidUntil, &old.Previous, &priorManifest, &old.SkippedForwardFrom)
	if err != nil {
		return err
	}
	if old.Scope != c.Scope || old.Project != c.Project || old.Offer >= c.Sequence || old.Draft == c.Draft || old.Fence != cmd.Fence || old.Decision != cmd.Previous || phase != c.Phase || old.Business != c.Business {
		return nil
	}
	read := pgDNSRouteSourceReader(ctx, tx)
	if err = s.checkHostedLiveHolds(ctx, tx, read, p, full, time.Now().UTC()); err != nil {
		return err
	}
	priorProposal, err := s.hostedHeadProposal(read, c.Host, old.Scope, old.Draft, hash, old.Authority, old.ValidUntil)
	if err != nil {
		return err
	}
	if !sameHostedProposalAcrossParents(priorProposal, p) {
		return nil
	}
	oldFull, err := read.artifact(priorProposal.Base.ReleaseSetID)
	if err != nil {
		return err
	}
	if oldFull.ID != priorProposal.Base.ReleaseSetID || oldFull.ScopeKey != pc.GlobalScopeKey || oldFull.ArtifactKind != model.PlatformArtifactKindReleaseSet ||
		oldFull.ContentHash != priorProposal.Base.ReleaseSetDigest || !platformsafety.EvaluateArtifactIntegrity(oldFull, s.platformArtifactSigningKeyring()).Pass {
		return ErrConflict
	}
	oldUnit, err := routeartifact.OpenHostedPhase(priorProposal, hash, image, body, phase)
	if err != nil {
		return err
	}
	oldSet := hostedDeliverySet{}
	oldParts, oldDirectory, err := pgHostedCurrentManifest(ctx, tx, oldFull, priorProposal, oldUnit, nil, nil, nil, oldSet.add)
	if err != nil {
		return err
	}
	members, prior, err := hostedSelectionManifests(manifest, priorManifest)
	if err != nil {
		return err
	}
	if len(oldParts) == 0 || !maps.Equal(oldParts, members) || len(nextSet) != len(oldSet) {
		return nil
	}
	j, err := pgHostedRebindJournal(ctx, tx, c.Host, true)
	if err != nil {
		return err
	}
	// Compare the nonselected phase too. In particular a forward cannot
	// acquire a different rollback program merely because it never served it.
	other := "prepare"
	if phase == "prepare" {
		other = "forward"
	}
	phaseMap := func(f model.PlatformArtifact, proposal pc.HostedScopeProposal, draft, pin string, b []byte) (hostedDeliverySet, error) {
		u, e := routeartifact.OpenHostedPhase(proposal, draft, pin, b, other)
		if e != nil {
			return nil, e
		}
		v := hostedDeliverySet{}
		parts, _, e := pgHostedCurrentManifest(ctx, tx, f, proposal, u, nil, nil, nil, v.add)
		if e != nil || len(parts) == 0 {
			return nil, e
		}
		return v, nil
	}
	oldOther, err := phaseMap(oldFull, priorProposal, hash, image, body)
	if err != nil {
		return err
	}
	newOther, err := phaseMap(full, p, c.DraftDigest, program.Digest, program.Wire)
	if err != nil {
		return err
	}
	if len(oldOther) != len(oldSet) || len(newOther) != len(nextSet) {
		return nil
	}
	w := hostedRebindWrite{Next: cmd, OldDraft: old.Draft, OldProgram: image, OldAuthority: old.Authority, OldDirectory: oldDirectory, Directory: directory}
	for k, current := range nextSet {
		v, ok := oldSet[k]
		if !ok {
			return nil
		}
		past := old
		past.Parent, past.Child, past.ChildDigest, past.Delivery = v.assignment.ReleaseSetID, v.assignment.ArtifactID, v.assignment.ContentHash, v.delivery
		past.MemberBaseline = oldUnit.MemberBaseline(v.delivery)
		past, err = hostedWirePredecessor(past, members, prior, j)
		if err != nil {
			return err
		}
		pin, e := routeartifact.HostedContinuationPredecessor(past, current.delivery, nextUnit.MemberBaseline(current.delivery))
		if e != nil {
			return nil
		}
		a, b := oldOther[k].delivery, newOther[k].delivery
		if len(a.Parts) != len(v.delivery.Parts) || len(b.Parts) != len(current.delivery.Parts) {
			return nil
		}
		for i, part := range current.delivery.Parts {
			prev := past.Delivery.Parts[i]
			if a.Parts[i].Recipient != prev.Recipient || b.Parts[i].Recipient != part.Recipient || a.Parts[i].ContentDigest != b.Parts[i].ContentDigest {
				return nil
			}
			prevKey, e := v.delivery.PartKey(prev)
			if e != nil {
				return e
			}
			key, e := current.delivery.PartKey(part)
			if e != nil {
				return e
			}
			prepared := part.ContentDigest
			if phase == "forward" {
				prepared = b.Parts[i].ContentDigest
			}
			w.Slots = append(w.Slots, hostedRebindSlot{prevKey, key, part.ContentDigest, prepared, pin})
		}
	}
	_, err = pgRebindHostedPhase(ctx, tx, w)
	// A predecessor not yet observed by every member leaves the new offer
	// intact. It is not a successful rebind or a blocking publication error.
	if errors.Is(err, ErrConflict) {
		return nil
	}
	return err
}
