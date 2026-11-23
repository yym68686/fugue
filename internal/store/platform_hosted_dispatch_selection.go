package store

import (
	"context"
	"database/sql"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

// Server-derived pin, never an API credential. An identical artifact in a
// different lane or another verification of the same bytes is not this pin.
type HostedDispatchSelection struct {
	Scope               string
	Full                model.PlatformPublicationPrecondition
	LKGID, EvidenceHash string
	ExpiresAt           time.Time
}

// Verify every participating full lane in ONE database snapshot. Do not
// re-sign other routes, renew their LKG, or select any candidate here.
func (s *Store) ValidateHostedDispatchSelections(ctx context.Context, pins []HostedDispatchSelection) error {
	if s == nil || len(pins) == 0 || len(pins) > 65 {
		return ErrInvalidInput
	}
	seen := map[string]bool{}
	for _, p := range pins {
		if p.Scope == "" || seen[p.Scope] || p.Full.FencingToken <= 0 || p.LKGID == "" || p.EvidenceHash == "" || p.ExpiresAt.IsZero() {
			return ErrInvalidInput
		}
		seen[p.Scope] = true
	}
	keys := s.platformArtifactSigningKeyring()
	check := func(read dnsRouteSourceReader) error {
		for _, p := range pins {
			if err := ctx.Err(); err != nil {
				return err
			}
			a, r, err := dnsProducerSelection(read, model.PlatformArtifactKindReleaseSet, p.Scope, model.PlatformArtifactReleaseChannelFull, p.Full, keys)
			if err != nil || r.VerificationState != model.PlatformArtifactVerificationStateVerified || r.VerifiedLKGGeneration != a.Generation {
				return ErrConflict
			}
			if p.Scope == pc.GlobalScopeKey {
				if a.Content["publication_role"] != nil && a.Content["publication_role"] != "" {
					return ErrConflict
				}
			} else {
				topology, err := pc.TrafficConsumersFromRelease(a)
				if err != nil || topology == nil || pc.AuthorityCellScope(topology.AuthorityCellID) != p.Scope || a.Content["publication_role"] != pc.PublicationRoleCellRoutes {
					return ErrConflict
				}
			}
			lkg, err := read.lkg(p.Scope)
			if err != nil || lkg == nil || lkg.ID != p.LKGID || lkg.VerifiedByReleaseID != r.ID || lkg.VerificationEvidenceHash != p.EvidenceHash || !lkg.ExpiresAt.Equal(p.ExpiresAt) ||
				!platformsafety.EvaluatePlatformLKGSnapshot(*lkg, a, keys, time.Now().UTC()).Pass {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		for _, p := range pins {
			if !p.ExpiresAt.After(now) {
				return ErrConflict
			}
		}
		return ctx.Err()
	}
	if s.db == nil {
		return s.withLockedState(false, func(v *model.State) error { return check(stateDNSRouteSourceReader(v)) })
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := check(pgDNSRouteSourceReader(ctx, tx)); err != nil {
		return err
	}
	return tx.Commit()
}
