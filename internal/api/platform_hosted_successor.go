package api

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/store"
)

// Select one host from actual changed root/managed paths, in stable order.
// B with a newer independent update is read from the selected full on every
// invocation. Never restore an earlier global full to undo A's paths.
func (s *Server) compileOfferedHostedSuccessor(ctx context.Context, req pc.CompileRequest, source pc.HostedAddressSource, capture *pc.HostedTransitionCapture) (pc.CompileResult, string, bool, error) {
	var empty pc.CompileResult
	prepare, eligible := map[string]bool{}, map[string]bool{}
	if capture != nil {
		for _, h := range capture.Hosts {
			prepare[h], eligible[h] = true, true
		}
	}
	for _, r := range source.Inherited {
		if r.Record.RecordKind == "hosted" {
			eligible[r.Record.Hostname] = true
		}
	}
	paths := func(intent pc.PlatformIntent, h string) map[string]string {
		out := map[string]string{}
		for _, r := range intent.Routes {
			if r.Hostname == h {
				out[model.NormalizeAppRoutePathPrefix(r.PathPrefix)] = r.AppID
			}
		}
		return out
	}
	var blocked error
	for _, host := range slices.Sorted(maps.Keys(eligible)) {
		wanted := paths(req.Intent, host)
		if maps.Equal(wanted, paths(source.PriorIntent, host)) {
			continue
		}
		for _, r := range source.Prior.Records {
			if r.Hostname != host || r.RecordKind != "hosted" || (r.Type != "A" && r.Type != "AAAA") {
				continue
			}
			claim, p, dir, found, e := s.store.ReadHostedSuccessor(ctx, host, r.TenantID, r.AppID, func(in store.ProjectHostRouteInputs, p pc.HostedScopeProposal) (string, string, string, error) {
				b, e := matchHostedProjectIntent(in, p)
				return b.OwnerDigest, b.PathDigest, b.Direction, e
			})
			if e != nil {
				if !errors.Is(e, store.ErrConflict) {
					return empty, "", false, e
				}
				blocked = errors.Join(blocked, e)
				break
			}
			if !found {
				if prepare[host] {
					blocked = errors.Join(blocked, errors.New("host preparation has no current offer"))
				}
				break
			}
			rows := p.After
			claim.Phase = "forward"
			if dir == "baseline" {
				claim.Phase = "withdraw"
				rows = p.Prepared
			} else if prepare[host] {
				claim.Phase = "prepare"
			}
			if len(wanted) != len(rows.Routes) || !source.Authority.ExpiresAt.After(time.Now()) {
				return empty, "", false, store.ErrConflict
			}
			for _, rt := range rows.Routes {
				if wanted[rt.PathPrefix] != rt.AppID {
					return empty, "", false, store.ErrConflict
				}
			}
			if claim.Phase == "prepare" && !p.Authority.Same(source.Authority) {
				return empty, "", false, store.ErrConflict
			}
			// For a follow-on phase the draft is historical configuration,
			// not today's authority. The exact current, verified full and
			// independently reconciled fresh input must reproduce the phase.
			data, e := json.Marshal(claim)
			if e != nil {
				return empty, "", false, e
			}
			// Include the exact offer and direction in immutable input lineage.
			req.RuntimeSnapshot.Facts = maps.Clone(req.RuntimeSnapshot.Facts)
			if req.RuntimeSnapshot.Facts == nil {
				return empty, "", false, errors.New("missing hosted producer input binding")
			}
			req.RuntimeSnapshot.Facts[store.HostedSuccessorMetadata] = string(data)
			result, e := pc.CompileHostedSuccessor(req, source, p, claim.Phase)
			if e != nil {
				blocked = errors.Join(blocked, e)
				break
			}
			return result, string(data), true, e
		}
	}
	return empty, "", false, blocked
}
