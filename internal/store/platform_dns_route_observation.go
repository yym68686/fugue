package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/cellpublication"
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

type dnsRouteSourceReader struct {
	artifact func(string) (model.PlatformArtifact, error)
	release  func(string) (model.PlatformArtifactRelease, error)
	lane     func(string) (model.PlatformReleaseLane, error)
	lkg      func(string) (*model.PlatformLKGSnapshot, error)
}

// ObserveDNSRouteSources reads only the selected lanes and current LKG in
// approved scopes. PostgreSQL uses one read-only repeatable-read transaction;
// no row/advisory write locks, mutations or historical scans are needed. The
// consumer API independently rechecks its DNS assignment before disclosure.
func (s *Store) ObserveDNSRouteSources(ctx context.Context, child model.PlatformArtifact) (model.PlatformDNSRouteSourceSnapshot, error) {
	keys := s.platformArtifactSigningKeyring()
	if child.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || child.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(child, keys).Pass {
		return model.PlatformDNSRouteSourceSnapshot{}, ErrConflict
	}
	if _, err := cellpublication.VerifyDNSArtifact(child, keys); err != nil {
		return model.PlatformDNSRouteSourceSnapshot{}, fmt.Errorf("%w: %s", ErrConflict, err)
	}
	var result model.PlatformDNSRouteSourceSnapshot
	if s.db == nil {
		err := s.withLockedState(false, func(state *model.State) error {
			read := dnsRouteSourceReader{
				artifact: func(id string) (model.PlatformArtifact, error) {
					i := platformArtifactIndex(state.PlatformArtifacts, id)
					if i < 0 {
						return model.PlatformArtifact{}, ErrNotFound
					}
					return state.PlatformArtifacts[i], nil
				},
				release: func(id string) (model.PlatformArtifactRelease, error) {
					i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, id)
					if i < 0 {
						return model.PlatformArtifactRelease{}, ErrNotFound
					}
					return state.PlatformArtifactReleases[i], nil
				},
				lane: func(id string) (model.PlatformReleaseLane, error) {
					v, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, id)
					if !ok {
						return v, ErrNotFound
					}
					return v, nil
				},
				lkg: func(scope string) (*model.PlatformLKGSnapshot, error) {
					return platformLKGSnapshotForScope(state.PlatformLKGSnapshots, model.PlatformArtifactKindReleaseSet, scope), nil
				},
			}
			var err error
			result, err = observeDNSRouteSources(ctx, child, read, keys, time.Now().UTC())
			return err
		})
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	result, err = observeDNSRouteSources(ctx, child, pgDNSRouteSourceReader(ctx, tx), keys, time.Now().UTC())
	if err != nil {
		return result, err
	}
	return result, tx.Commit()
}

func observeDNSRouteSources(ctx context.Context, child model.PlatformArtifact, read dnsRouteSourceReader, keys bundleauth.Keyring, now time.Time) (model.PlatformDNSRouteSourceSnapshot, error) {
	out := model.PlatformDNSRouteSourceSnapshot{DNSArtifactID: child.ID, DNSArtifactDigest: child.ContentHash, ObservedAt: now, Scopes: []model.PlatformDNSRouteSourceScope{}}
	approved, err := platformconfig.DNSRouteSourceAuthorizations(child)
	if err != nil || len(approved) == 0 {
		return out, ErrNotFound
	}
	scopes := []string{}
	for _, source := range approved {
		if !slices.Contains(scopes, source.ScopeKey) {
			scopes = append(scopes, source.ScopeKey)
		}
	}
	slices.Sort(scopes)
	if len(scopes) > 17 {
		return out, ErrConflict
	}
	// A candidate and LKG often share artifacts. Reuse only within this coherent
	// snapshot; a later request must read current validation and selection again.
	cache := map[string]model.PlatformArtifact{}
	bytesRead := 0
	readArtifact := func(id string) (model.PlatformArtifact, error) {
		if a, ok := cache[id]; ok {
			return a, nil
		}
		if err := ctx.Err(); err != nil {
			return model.PlatformArtifact{}, err
		}
		a, err := read.artifact(id)
		if err != nil {
			return a, err
		}
		raw, err := json.Marshal(a)
		if err != nil {
			return a, err
		}
		bytesRead += len(raw)
		if bytesRead > 16<<20 {
			return model.PlatformArtifact{}, fmt.Errorf("%w: routing source observation exceeds bound", ErrConflict)
		}
		cache[id] = a
		return a, nil
	}
	for _, scope := range scopes {
		row := model.PlatformDNSRouteSourceScope{ScopeKey: scope, Lanes: []model.PlatformDNSRouteSourceLane{}, Publications: []model.PlatformDNSRouteSourcePublication{}}
		add := func(release model.PlatformArtifactRelease, selection string, lkg *model.PlatformLKGSnapshot) error {
			if release.ScopeKey != scope || release.ArtifactKind != model.PlatformArtifactKindReleaseSet {
				return ErrConflict
			}
			if release.VerificationState == model.PlatformArtifactVerificationStateFailed || release.Status == model.PlatformArtifactReleaseStatusRolledBack {
				return nil
			}
			for i, p := range row.Publications {
				if p.Release.ID == release.ID {
					row.Publications[i].Selections = append(row.Publications[i].Selections, selection)
					row.Publications[i].LKG = lkg
					return dnsroutesource.VerifyPublication(row.Publications[i], approved, keys, now)
				}
			}
			parent, err := readArtifact(release.ArtifactID)
			if err != nil {
				return err
			}
			if parent.ScopeKey != scope || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
				return ErrConflict
			}
			policyReleaseID := parent.Metadata[platformproducer.PolicyReleaseMetadata]
			if policyReleaseID == "" {
				return nil
			}
			producer, err := read.release(policyReleaseID)
			if err != nil {
				return err
			}
			policy, err := readArtifact(producer.ArtifactID)
			if err != nil {
				return err
			}
			if !slices.ContainsFunc(approved, func(a platformconfig.DNSRouteSourceAuthorization) bool {
				return a.ScopeKey == scope && a.PolicyArtifactID == policy.ID && a.PolicyDigest == policy.ContentHash
			}) {
				return nil
			}
			p := model.PlatformDNSRouteSourcePublication{Selections: []string{selection}, Parent: parent, Release: release, ProducerPolicy: policy, ProducerRelease: producer, LKG: lkg}
			var set platformconfig.ReleaseSet
			raw, _ := json.Marshal(parent.Content)
			if json.Unmarshal(raw, &set) != nil || len(set.ArtifactIDs) != len(set.ArtifactKinds) {
				return ErrConflict
			}
			for i, kind := range set.ArtifactKinds {
				if kind != model.PlatformArtifactKindEdgeRouteBundle && kind != model.PlatformArtifactKindCaddyRouteConfig {
					continue
				}
				a, err := readArtifact(set.ArtifactIDs[i])
				if err != nil {
					return err
				}
				if kind == model.PlatformArtifactKindEdgeRouteBundle {
					p.Route = a
				} else {
					p.TLS = a
				}
			}
			if err := dnsroutesource.VerifyPublication(p, approved, keys, now); err != nil {
				return fmt.Errorf("%w: %s", ErrConflict, err)
			}
			row.Publications = append(row.Publications, p)
			return nil
		}
		for _, channel := range []string{"gray", "full"} {
			key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, scope, channel)
			lane, err := read.lane(key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return out, err
			}
			if lane.LaneKey != key || lane.ScopeKey != scope || lane.ReleaseChannel != channel || lane.ArtifactKind != model.PlatformArtifactKindReleaseSet || lane.Frozen || lane.FencingToken <= 0 || lane.Version <= 0 {
				return out, ErrConflict
			}
			row.Lanes = append(row.Lanes, model.PlatformDNSRouteSourceLane{ReleaseChannel: channel, FencingToken: lane.FencingToken, Version: lane.Version, ActiveReleaseID: lane.ActiveReleaseID})
			if lane.ActiveReleaseID == "" {
				continue
			}
			release, err := read.release(lane.ActiveReleaseID)
			if err != nil {
				return out, err
			}
			if release.LaneKey != key || release.FencingToken != lane.FencingToken || release.ReleaseChannel != channel || release.Status != model.PlatformArtifactReleaseStatusActive {
				return out, ErrConflict
			}
			if err := add(release, channel, nil); err != nil {
				return out, err
			}
		}
		lkg, err := read.lkg(scope)
		if err != nil {
			return out, err
		}
		if lkg != nil && lkg.ExpiresAt.After(now) {
			parent, err := readArtifact(lkg.ArtifactID)
			if err != nil {
				return out, err
			}
			if lkg.ScopeKey != scope || lkg.ArtifactKind != model.PlatformArtifactKindReleaseSet || !platformLKGSnapshotMatchesArtifact(*lkg, parent, now, keys) {
				return out, ErrConflict
			}
			release, err := read.release(lkg.VerifiedByReleaseID)
			if err != nil {
				return out, err
			}
			if err := add(release, "lkg", lkg); err != nil {
				return out, err
			}
		}
		if len(row.Publications) == 0 {
			return out, ErrNotFound
		}
		out.Scopes = append(out.Scopes, row)
	}
	out.SelectionDigest, err = dnsroutesource.SelectionDigest(out)
	return out, err
}

func pgDNSRouteSourceReader(ctx context.Context, tx *sql.Tx) dnsRouteSourceReader {
	return dnsRouteSourceReader{
		artifact: func(id string) (model.PlatformArtifact, error) {
			return pgGetPlatformArtifactForUpdate(ctx, tx, id, false)
		},
		release: func(id string) (model.PlatformArtifactRelease, error) {
			return pgGetPlatformArtifactRelease(ctx, tx, id, false)
		},
		lane: func(id string) (model.PlatformReleaseLane, error) {
			return scanPlatformReleaseLane(tx.QueryRowContext(ctx, `SELECT lane_key, artifact_kind, scope_key, release_channel, fencing_token, version, active_release_id, frozen, freeze_reason, updated_at FROM fugue_platform_release_lanes WHERE lane_key=$1`, id))
		},
		lkg: func(scope string) (*model.PlatformLKGSnapshot, error) {
			v, err := scanPlatformLKGSnapshot(tx.QueryRowContext(ctx, `SELECT id, artifact_id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, content_hash, artifact_provenance_json, verified_by_release_id, verification_evidence_hash, snapshot_provenance_json, expires_at, created_at, updated_at FROM fugue_platform_lkg_snapshots WHERE artifact_kind=$1 AND scope_key=$2`, model.PlatformArtifactKindReleaseSet, scope))
			if errors.Is(err, ErrNotFound) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			return &v, nil
		},
	}
}
