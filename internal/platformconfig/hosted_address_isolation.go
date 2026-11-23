package platformconfig

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// Permanent reservations are not a global change freeze. Once the same
// retained owners have the same active paths, ordinary unrelated intent can
// use its normal publication authority. Enforce isolation specifically when
// first separating a hold from paths, or when those paths actually change.
func ValidateScopedHostedPublicationTransition(previous, next model.PlatformArtifact) error {
	ref, _, err := HostedAddressAuthority(next)
	if err != nil || ref == nil {
		return err
	}
	old, _, err := HostedAddressAuthority(previous)
	if err != nil {
		return err
	}
	if old == nil {
		return ValidateHostedPublicationIsolation(previous, next)
	}
	type owner struct{ host, family, app, tenant string }
	read := func(a model.PlatformArtifact) (map[owner]map[string]bool, error) {
		payload, err := DecodeDNSArtifact(a)
		if err != nil || payload.ReadinessPlan == nil {
			return nil, errors.New("hosted address transition lacks its signed DNS plan")
		}
		type key struct{ host, family string }
		counts := map[key]int{}
		paths := map[string]map[string]bool{}
		for _, r := range payload.ReadinessPlan.Records {
			for _, t := range r.Targets {
				counts[key{r.Hostname, t.Family}]++
			}
		}
		for _, p := range payload.ReadinessPlan.Probes {
			if paths[p.Hostname] == nil {
				paths[p.Hostname] = map[string]bool{}
			}
			paths[p.Hostname][p.Path] = true
		}
		selected := map[owner]map[string]bool{}
		for _, r := range payload.Records {
			n := counts[key{r.Hostname, r.Type}]
			if r.RecordKind == "hosted" && (r.Type == "A" || r.Type == "AAAA") && n > 0 && len(r.Values) > n {
				if len(paths[r.Hostname]) == 0 {
					return nil, errors.New("reserved address has no active route path")
				}
				selected[owner{r.Hostname, r.Type, r.AppID, r.TenantID}] = paths[r.Hostname]
			}
		}
		if len(selected) == 0 {
			return nil, errors.New("reservation metadata has no matching physical record")
		}
		return selected, nil
	}
	before, e := read(previous)
	if e != nil {
		return e
	}
	after, e := read(next)
	if e != nil {
		return e
	}
	for k, paths := range after {
		if !maps.Equal(paths, before[k]) {
			return ValidateHostedPublicationIsolation(previous, next)
		}
	}
	return nil
}

// A managed path release cannot donate its authority to unrelated pending
// DNS edits. Compare the complete signed declarations, not only the path
// owner or the number of records. Ordering of RR values is not an update.
func ValidateHostedPublicationIsolation(previous, next model.PlatformArtifact) error {
	bad := errors.New("hosted path publication also changes the global DNS declarations")
	read := func(a model.PlatformArtifact) (map[[2]string]DNSIntent, error) {
		if a.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
			return nil, bad
		}
		raw, ok := a.Content["records"].([]any)
		if !ok || len(raw) < 1 || len(raw) > 20000 {
			return nil, bad
		}
		out := make(map[[2]string]DNSIntent, len(raw))
		for _, v := range raw {
			var r DNSIntent
			b, e := json.Marshal(v)
			if e != nil || json.Unmarshal(b, &r) != nil || r.Hostname == "" || r.Type == "" {
				return nil, bad
			}
			k := [2]string{r.Hostname, r.Type}
			if _, dup := out[k]; dup {
				return nil, bad
			}
			r.Values = slices.Clone(r.Values)
			slices.Sort(r.Values)
			out[k] = r
		}
		return out, nil
	}
	left, e := read(previous)
	if e != nil {
		return e
	}
	right, e := read(next)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(left, right) {
		return bad
	}
	return nil
}
