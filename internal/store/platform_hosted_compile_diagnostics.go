package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// This is a bounded, replaceable status hint from the normal producer. It
// cannot create a draft, affect a selection or outlive its parent authority.
const hostedCompileDiagnosticsKey = "hosted-compile-diagnostics-v1"
const hostedCompileDiagnosticsMaxBytes = 64 << 10

type hostedCompileDiagnostic struct {
	Scope    string
	Paths    string
	Conflict model.ProjectHostCompileConflict
}

type hostedCompileDiagnostics struct {
	Base      pc.PreviousTrafficPublicationReference
	Authority pc.HostedProposalAuthority
	Entries   []hostedCompileDiagnostic
}

func hostedExternalRefusals(err error) []*pc.HostedExternalCompileRefusal {
	var found []*pc.HostedExternalCompileRefusal
	var visit func(error)
	visit = func(e error) {
		switch x := e.(type) {
		case *pc.HostedExternalCompileRefusal:
			found = append(found, x)
		case interface{ Unwrap() []error }:
			for _, child := range x.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(x.Unwrap())
		}
	}
	visit(err)
	return found
}

// The producer has already validated both selections. The atomically
// replaced row is a complete snapshot of the last capture, not a history or
// a per-domain table. A slow reconcile cannot replace newer evidence.
func (s *Store) rememberHostedExternalRefusals(ctx context.Context, src pc.HostedAddressSource, captured time.Time, refs []*pc.HostedExternalCompileRefusal) error {
	if !s.usingDatabase() {
		return nil
	}
	now := time.Now().UTC()
	if len(refs) > 64 || captured.IsZero() || captured.After(now) || now.Sub(captured) > 5*time.Minute || !src.Authority.ExpiresAt.After(now) {
		return ErrInvalidInput
	}
	captured = captured.UTC().Truncate(time.Microsecond)
	report := hostedCompileDiagnostics{Base: src.Publication, Authority: src.Authority}
	for _, item := range refs {
		c := item.Conflict
		if item.Host == "" || item.TenantID == "" || item.AppID == "" || !platformproducer.ValidDigest(item.DesiredPathsDigest) ||
			c == nil || c.Target == "" || (c.RecordType != "A" && c.RecordType != "AAAA") {
			return ErrInvalidInput
		}
		_, key := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: item.Host, TenantID: item.TenantID, AppID: item.AppID})
		report.Entries = append(report.Entries, hostedCompileDiagnostic{Scope: key, Paths: item.DesiredPathsDigest, Conflict: model.ProjectHostCompileConflict{
			ParentReleaseID: src.Publication.ReleaseID, ObservedAt: captured.UTC(), Target: c.Target, RecordType: c.RecordType, Field: c.Field,
			ExpectedAddresses: c.ExpectedAddresses, CandidateAddresses: c.CandidateAddresses,
		}})
	}
	// A capture with no refusals still records a tombstone. Without it a
	// slower capture could publish an old refusal after a successful one.
	body := ""
	if len(report.Entries) > 0 {
		raw, err := json.Marshal(report)
		if err != nil || len(raw) > hostedCompileDiagnosticsMaxBytes {
			return ErrInvalidInput
		}
		body = string(raw)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO fugue_meta(key,value,updated_at) VALUES($1,$2,$3)
 ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at
 WHERE fugue_meta.updated_at<EXCLUDED.updated_at`, hostedCompileDiagnosticsKey, body, captured)
	return err
}

func (s *Store) pgHostedExternalRefusalStatus(ctx context.Context, tx *sql.Tx, project model.Project, plan model.ProjectHostRoutePlan, key string) (model.ProjectHostRoutePublicationStatus, error) {
	var empty model.ProjectHostRoutePublicationStatus
	var raw string
	var captured time.Time
	err := tx.QueryRowContext(ctx, `SELECT value,updated_at FROM fugue_meta WHERE key=$1 AND octet_length(value) BETWEEN 1 AND $2
 AND updated_at<=statement_timestamp() AND updated_at>statement_timestamp()-interval '5 minutes'`,
		hostedCompileDiagnosticsKey, hostedCompileDiagnosticsMaxBytes).Scan(&raw, &captured)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, nil
	}
	if err != nil {
		return empty, err
	}
	var report hostedCompileDiagnostics
	if json.Unmarshal([]byte(raw), &report) != nil || len(report.Entries) > 64 || !report.Authority.ExpiresAt.After(time.Now()) || report.Base.Validate() != nil {
		return empty, nil
	}
	// Check the observation's exact desired and selected identities before
	// returning it, even though the caller already verified the saved paths.
	want, err := pc.Digest(plan.After)
	if err != nil || project.ID != plan.ProjectID {
		return empty, nil
	}
	c := matchingHostedCompileDiagnostic(report, key, want, captured)
	if c == nil {
		return empty, nil
	}
	if _, err := s.hostedSelectedBase(pgDNSRouteSourceReader(ctx, tx), pc.HostedScopeProposal{Base: report.Base, Authority: report.Authority}); err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
			return empty, nil
		}
		return empty, err
	}
	return model.ProjectHostRoutePublicationStatus{
		State: "pending", Reason: "scoped compiler rejected the shared DNS target before a draft was prepared", HostCompileConflict: c,
	}, nil
}

func matchingHostedCompileDiagnostic(report hostedCompileDiagnostics, scope, paths string, captured time.Time) *model.ProjectHostCompileConflict {
	for _, item := range report.Entries {
		c := item.Conflict
		if item.Scope == scope && item.Paths == paths && c.ParentReleaseID == report.Base.ReleaseID && c.ObservedAt.UTC().Truncate(time.Microsecond).Equal(captured) {
			return &c
		}
	}
	return nil
}
