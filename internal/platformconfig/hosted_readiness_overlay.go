package platformconfig

import (
	"encoding/json"
	"errors"
	"slices"
	"sort"
)

// ComposeHostedReadinessRequirements evaluates the host's *requirements*
// over a particular previously selected plan. Unrelated requirements come
// from that base, never from the candidate. The result contains no consumer
// observations or leases and cannot grant, refresh or roll back a release.
// Callers must keep the original base authority and its expiry; an overlay
// must be discarded if that authority is replaced, expired or withdrawn.
func ComposeHostedReadinessRequirements(host string, base, desired *DNSReadinessPlan, policy *DNSReadinessPolicy, beforePaths, afterPaths []string) (*DNSReadinessPlan, HostedReadinessDelta, error) {
	return composeHostedReadinessAt(host, host, base, desired, policy, beforePaths, afterPaths)
}

func composeHostedReadinessAt(host, anchor string, base, desired *DNSReadinessPlan, policy *DNSReadinessPolicy, beforePaths, afterPaths []string) (*DNSReadinessPlan, HostedReadinessDelta, error) {
	d, err := planHostedReadinessDelta(host, anchor, base, desired, policy, beforePaths, afterPaths)
	if err != nil {
		return nil, HostedReadinessDelta{}, err
	}
	return applyHostedReadinessDelta(host, base, desired.Probes, policy, d)
}

// A recipient has the current plan and the selected host's new probes, not
// the producer's global candidate. Classify existing IDs by the current plan:
// a candidate must never be able to reclassify a peer's proof as its own.
func applyHostedReadinessDelta(host string, base *DNSReadinessPlan, replacements []DNSReadinessProbe, policy *DNSReadinessPolicy, d HostedReadinessDelta) (*DNSReadinessPlan, HostedReadinessDelta, error) {
	bad := errors.New("hosted requirements overlay does not exactly cover the dependency closure")
	if base == nil || ValidateDNSReadinessPlan(base, policy) != nil {
		return nil, HostedReadinessDelta{}, bad
	}
	type key struct{ record, edge, address string }
	edits := make(map[key]HostedReadinessTargetEdit, len(d.Edits))
	for _, e := range d.Edits {
		k := key{e.Record, e.Edge, e.Address}
		if _, dup := edits[k]; dup {
			return nil, HostedReadinessDelta{}, bad
		}
		edits[k] = e
	}
	dict := make(map[string]DNSReadinessProbe, len(base.Probes))
	baseOwners := make(map[string]string, len(base.Probes))
	for _, p := range base.Probes {
		dict[p.ID] = p
		baseOwners[p.ID] = p.Hostname
	}
	for _, p := range replacements {
		if p.Hostname == host {
			if owner, ok := baseOwners[p.ID]; ok && owner != host {
				return nil, HostedReadinessDelta{}, bad
			}
			dict[p.ID] = p
		}
	}
	view := DNSReadinessPlan{Records: slices.Clone(base.Records)}
	used := map[string]bool{}
	for i, r := range view.Records {
		r.Targets = slices.Clone(r.Targets)
		for j, t := range r.Targets {
			k := key{r.Hostname, t.EdgeID, t.Address}
			if e, ok := edits[k]; ok {
				var owned, keep []string
				for _, id := range t.ProbeIDs {
					if baseOwners[id] == host {
						owned = append(owned, id)
					} else {
						keep = append(keep, id)
					}
				}
				slices.Sort(owned)
				if !slices.Equal(owned, e.Before) {
					return nil, HostedReadinessDelta{}, bad
				}
				t.ProbeIDs = append(keep, e.After...)
				slices.Sort(t.ProbeIDs)
				r.Targets[j] = t
				delete(edits, k)
			}
			for _, id := range t.ProbeIDs {
				p, ok := dict[id]
				if !ok {
					return nil, HostedReadinessDelta{}, bad
				}
				if !used[id] {
					used[id] = true
					view.Probes = append(view.Probes, p)
				}
			}
		}
		view.Records[i] = r
	}
	if len(edits) != 0 || ValidateDNSReadinessPlan(&view, policy) != nil {
		return nil, HostedReadinessDelta{}, bad
	}
	sort.Slice(view.Probes, func(i, j int) bool { return view.Probes[i].ID < view.Probes[j].ID })
	// Return owned data: readers can discard or mutate their view without
	// changing the immutable sources (including nested policy maps/pointers).
	raw, err := json.Marshal(view)
	var out DNSReadinessPlan
	if err != nil || json.Unmarshal(raw, &out) != nil {
		return nil, HostedReadinessDelta{}, bad
	}
	return &out, d, nil
}
