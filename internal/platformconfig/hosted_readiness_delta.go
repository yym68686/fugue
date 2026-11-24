package platformconfig

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"sort"

	"fugue/internal/model"
)

// HostedReadinessDelta describes requirement edits, not observations, serving
// authority or a new full release. In particular it cannot renew a proof,
// create an RR, admit an address, or copy another owner's pending edits.
type HostedReadinessDelta struct {
	Host, BasePlanDigest, DesiredPlanDigest string
	Edits                                   []HostedReadinessTargetEdit
}

type HostedReadinessTargetEdit struct {
	Record  string   `json:"record"`
	Edge    string   `json:"edge"`
	Address string   `json:"address"`
	Before  []string `json:"before"` // content-addressed requirement IDs only
	After   []string `json:"after"`
}

// PlanHostedReadinessDelta is the pure planning step for a path-only
// publication. Follow reverse probe references: an alias can depend on the
// host despite having a different wire name. Refuse to splice a topology,
// quorum, delegation, health-state or freshness-policy change into the edit.
// Callers must independently verify both publications and the current route,
// DNS and TLS ownership; this function has no way to authorize serving.
func PlanHostedReadinessDelta(host string, base, want *DNSReadinessPlan, policy *DNSReadinessPolicy, beforePaths, afterPaths []string) (HostedReadinessDelta, error) {
	return planHostedReadinessDelta(host, host, base, want, policy, beforePaths, afterPaths)
}

// anchor is the name of the actual address record. For a managed domain
// that is the host; for an external domain it is the separately checked
// Fugue target. In neither case may a plan with no such record succeed.
func planHostedReadinessDelta(host, anchor string, base, want *DNSReadinessPlan, policy *DNSReadinessPolicy, beforePaths, afterPaths []string) (HostedReadinessDelta, error) {
	bad := errors.New("hosted readiness changes are not an isolated path edit")
	paths := func(v []string) map[string]bool {
		out := map[string]bool{}
		for _, p := range v {
			if p == "" || p != model.NormalizeAppRoutePathPrefix(p) || out[p] {
				return nil
			}
			out[p] = true
		}
		if !out["/"] || len(out) > 256 {
			return nil
		}
		return out
	}
	bp, wp := paths(beforePaths), paths(afterPaths)
	if !validDNSConsumerZone(host) || bp == nil || wp == nil || maps.Equal(bp, wp) || base == nil || want == nil ||
		ValidateDNSReadinessPlan(base, policy) != nil || ValidateDNSReadinessPlan(want, policy) != nil {
		return HostedReadinessDelta{}, bad
	}
	type key struct{ record, edge, address string }
	type part struct {
		ids   []string
		shape DNSReadinessProbe
	}
	type side struct {
		records map[string]DNSReadinessRecord
		owned   map[key]part
	}
	read := func(plan *DNSReadinessPlan, expected map[string]bool) (side, error) {
		s := side{map[string]DNSReadinessRecord{}, map[key]part{}}
		probes := map[string]DNSReadinessProbe{}
		for _, p := range plan.Probes {
			probes[p.ID] = p
		}
		for _, rr := range plan.Records {
			r := rr
			r.Targets = slices.Clone(r.Targets)
			relevant := false
			for i, t := range r.Targets {
				k := key{r.Hostname, t.EdgeID, t.Address}
				var field part
				foreign := []string{}
				got := map[string]bool{}
				for _, id := range t.ProbeIDs {
					p := probes[id]
					if p.Hostname != host {
						foreign = append(foreign, id)
						continue
					}
					if p.State != "" || p.StaticEdge != nil || p.PreviousAuthority != nil || p.CellPublicationDigest != "" || got[p.Path] {
						return side{}, bad
					}
					got[p.Path] = true
					p.ID, p.Path, p.RouteDigest = "", "", ""
					if len(field.ids) > 0 && !reflect.DeepEqual(p, field.shape) {
						return side{}, bad
					}
					field.shape = p
					field.ids = append(field.ids, id)
				}
				if len(got) > 0 {
					if !maps.Equal(got, expected) {
						return side{}, bad
					}
					relevant = true
					slices.Sort(field.ids)
					s.owned[k] = field
				} else if r.Hostname == anchor {
					return side{}, bad
				}
				if anchor != host && r.Hostname == anchor {
					if t.RequireSinglePublication && len(foreign) > 0 {
						return side{}, bad
					}
					// Compare topology, not another host's path revision.
					// The overlay always copies these IDs from its current base.
					foreign = nil
				}
				slices.Sort(foreign)
				r.Targets[i].ProbeIDs = foreign
			}
			if relevant {
				sort.Slice(r.Targets, func(i, j int) bool {
					a, b := r.Targets[i], r.Targets[j]
					if a.EdgeID != b.EdgeID {
						return a.EdgeID < b.EdgeID
					}
					return a.Address < b.Address
				})
				s.records[r.Hostname] = r
			}
		}
		if _, ok := s.records[anchor]; !ok {
			return side{}, bad
		}
		if anchor != host {
			// An external edit cannot also change a second alias or a
			// managed RR. A shared target requiring one publication
			// cannot mix a host decision with its peers' global proofs.
			if len(s.records) != 1 {
				return side{}, bad
			}
		}
		return s, nil
	}
	b, err := read(base, bp)
	if err != nil {
		return HostedReadinessDelta{}, err
	}
	w, err := read(want, wp)
	if err != nil {
		return HostedReadinessDelta{}, err
	}
	if len(b.owned) != len(w.owned) || !reflect.DeepEqual(b.records, w.records) {
		return HostedReadinessDelta{}, bad
	}
	out := HostedReadinessDelta{Host: host}
	if out.BasePlanDigest, err = Digest(base); err != nil {
		return HostedReadinessDelta{}, err
	}
	if out.DesiredPlanDigest, err = Digest(want); err != nil {
		return HostedReadinessDelta{}, err
	}
	for k, v := range b.owned {
		to, ok := w.owned[k]
		if !ok || !reflect.DeepEqual(v.shape, to.shape) {
			return HostedReadinessDelta{}, bad
		}
		out.Edits = append(out.Edits, HostedReadinessTargetEdit{k.record, k.edge, k.address, v.ids, to.ids})
	}
	sort.Slice(out.Edits, func(i, j int) bool {
		a, b := out.Edits[i], out.Edits[j]
		if a.Record != b.Record {
			return a.Record < b.Record
		}
		if a.Edge != b.Edge {
			return a.Edge < b.Edge
		}
		return a.Address < b.Address
	})
	return out, nil
}
