package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/netip"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// The selected, verified full pins the exact policy the producer used
// to compute this host's eligible addresses. A new observation is not
// authority to invent a physical address or silently lift a recovery hold.
func (s *Store) checkHostedLiveHolds(ctx context.Context, tx *sql.Tx, read dnsRouteSourceReader, p pc.HostedScopeProposal, full model.PlatformArtifact, at time.Time) error {
	id, pin := full.Metadata[platformproducer.DNSPolicyIDMetadata], full.Metadata[platformproducer.DNSPolicyDigestMetadata]
	if id == "" || !platformproducer.ValidDigest(pin) {
		return ErrConflict
	}
	a, e := read.artifact(id)
	if e != nil {
		return e
	}
	if a.ID != id || a.ContentHash != pin || a.Status != model.PlatformArtifactStatusValidated || a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || !platformsafety.EvaluateArtifactIntegrity(a, s.platformArtifactSigningKeyring()).Pass {
		return ErrConflict
	}
	var declared []platformproducer.OfflineEdge
	if v, present := a.Content["offline_edges"]; present {
		raw, e := json.Marshal(v)
		if e != nil || v == nil || json.Unmarshal(raw, &declared) != nil || platformproducer.ValidateOfflineEdges(declared) != nil {
			return ErrConflict
		}
	}
	return pgCheckHostedPhysical(ctx, tx, p, declared, at)
}

func pgCheckHostedPhysical(ctx context.Context, tx *sql.Tx, p pc.HostedScopeProposal, declared []platformproducer.OfflineEdge, at time.Time) error {
	type request struct{ Node, Key, Member string }
	byNode := map[string][]pc.HostedAddressHold{}
	holds := map[string]platformproducer.OfflineEdge{}
	for _, h := range declared {
		holds[h.EdgeID] = h
	}
	var req []request
	for _, v := range p.Physical {
		ip, e := netip.ParseAddr(v.Address)
		if v.EdgeID == "" || v.EdgeGroupID == "" || e != nil || ip.Is4In6() || ip.String() != v.Address {
			return ErrConflict
		}
		byNode[v.EdgeID] = append(byNode[v.EdgeID], v)
		if len(byNode[v.EdgeID]) > 2 {
			return ErrConflict
		}
		if len(byNode[v.EdgeID]) > 1 {
			continue
		}
		q := request{Node: v.EdgeID}
		if h, ok := holds[v.EdgeID]; ok && h.Recovery != nil {
			enrollment := ""
			if h.WorkerEnrollment != nil {
				enrollment = h.WorkerEnrollment.ID
			}
			var e error
			q.Member, e = platformcontrol.EnrolledPlatformConsumerID(model.PlatformConsumerComponentEdgeWorker, h.EdgeID, platformcontrol.ConsumerAuthorityID(h.EdgeGroupID), pc.GlobalScopeKey, enrollment)
			if e != nil {
				return ErrConflict
			}
			q.Key = observationCheckpointKey(platformproducer.OfflineRecoveryKey(h))
		}
		req = append(req, q)
	}
	if len(req) == 0 || len(req) > 8192 {
		return ErrConflict
	}
	raw, e := json.Marshal(req)
	if e != nil {
		return e
	}
	// One statement observes only this host's physical inventory, eligible
	// recovery checkpoint and permanent enrollment retirement together.
	rows, e := tx.QueryContext(ctx, `SELECT n.id,n.edge_group_id,n.public_ipv4,n.public_ipv6,
 COALESCE(CASE WHEN octet_length(m.value)<=4096 THEN m.value END,''),EXISTS(
 SELECT 1 FROM fugue_platform_consumer_retirements r WHERE r.consumer_id=q."Member" AND r.scope_key='global' AND r.artifact_kind IN ($2,$3))
 FROM jsonb_to_recordset($1::jsonb) q("Node" text,"Key" text,"Member" text)
 JOIN fugue_edge_nodes n ON n.id=q."Node" LEFT JOIN fugue_meta m ON m.key=q."Key" AND q."Key"<>''`,
		string(raw), model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig)
	if e != nil {
		return e
	}
	defer rows.Close()
	var effective []pc.HostedAddressHold
	for rows.Next() {
		var n pc.HostedAddressHold
		var ipv6 string
		var data string
		var retired bool
		if e = rows.Scan(&n.EdgeID, &n.EdgeGroupID, &n.Address, &ipv6, &data, &retired); e != nil {
			return e
		}
		for _, v := range byNode[n.EdgeID] {
			if v.EdgeGroupID != n.EdgeGroupID || (v.Address != n.Address && v.Address != ipv6) {
				return ErrConflict
			}
		}
		delete(byNode, n.EdgeID)
		h, ok := holds[n.EdgeID]
		if !ok {
			continue
		}
		if h.EdgeGroupID != n.EdgeGroupID || h.PublicIPv4 != n.Address {
			return ErrConflict
		}
		var o platformproducer.OfflineRecoveryObservation
		if retired || data == "" || json.Unmarshal([]byte(data), &o) != nil || !o.Ready(h, at) {
			effective = append(effective, n)
		}
	}
	if e = rows.Err(); e != nil {
		return e
	}
	if len(byNode) != 0 || !hostedHoldsCurrent(p, effective) {
		return ErrConflict
	}
	return nil
}
