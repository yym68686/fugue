package platformconfig

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
)

// The source is loaded under the verified full's release and input fence.
// A stored selector describes the configuration that DNS was asked to use;
// it is not evidence that any endpoint is currently healthy. Answer-time
// quorum, evidence expiration and consumer verification remain mandatory.
func bindHostedQueryInputs(req *CompileRequest, source HostedAddressSource, owned map[string]bool) error {
	bad := errors.New("host DNS selector inputs do not form an exact owned view")
	if len(owned) == 0 || req.RuntimeSnapshot.PolicyGeneration != "" && req.RuntimeSnapshot.PolicyGeneration != req.Policy.Generation {
		return fmt.Errorf("%w: capture generation", bad)
	}
	type key struct{ node, host, family string }
	type pair struct {
		rule      DNSAnswerRule
		selection DNSSelectionObservation
	}
	index := func(rules []DNSAnswerRule, selections []DNSSelectionObservation) (map[key]pair, error) {
		if len(rules) != len(selections) || ValidateDNSAnswerRules(rules) != nil {
			return nil, bad
		}
		m := make(map[key]pair, len(rules))
		for _, r := range rules {
			k := key{r.NodeID, r.Hostname, r.Type}
			if _, ok := m[k]; ok || k.node == "" || k.host == "" {
				return nil, bad
			}
			m[k] = pair{rule: r}
		}
		for _, s := range selections {
			k := key{s.NodeID, s.Hostname, s.Type}
			p, ok := m[k]
			if !ok || p.selection.NodeID != "" {
				return nil, bad
			}
			p.selection = s
			m[k] = p
		}
		return m, nil
	}
	base, e := index(source.Prior.Policy.DNSAnswerRules, source.PriorDNSSelections)
	if e != nil {
		return fmt.Errorf("%w: selected inputs", e)
	}
	current, e := index(req.Policy.DNSAnswerRules, req.RuntimeSnapshot.DNSSelections)
	if e != nil {
		return fmt.Errorf("%w: candidate inputs", e)
	}
	// Paths cannot add a DNS consumer, zone, address family, or new hostname.
	// Fresh owned keys must cover exactly the same membership as the base.
	baseOwned, currentOwned := map[key]bool{}, map[key]bool{}
	for k := range base {
		if owned[k.host] {
			baseOwned[k] = true
		}
	}
	for k := range current {
		if owned[k.host] {
			currentOwned[k] = true
		}
	}
	if !maps.Equal(baseOwned, currentOwned) {
		return fmt.Errorf("%w: consumer or address-family membership", bad)
	}
	rules := make([]DNSAnswerRule, 0, len(base))
	selections := make([]DNSSelectionObservation, 0, len(base))
	for _, r := range source.Prior.Policy.DNSAnswerRules {
		k := key{r.NodeID, r.Hostname, r.Type}
		p := base[k]
		if owned[k.host] {
			p = current[k]
		}
		rules = append(rules, p.rule)
		selections = append(selections, p.selection)
	}
	original := NormalizePolicySnapshot(req.Policy)
	policy := original
	policy.DNSAnswerRules = rules
	policy = NormalizePolicySnapshot(policy)
	if !reflect.DeepEqual(original.DNSAnswerRules, policy.DNSAnswerRules) {
		policy.Generation, e = PolicySnapshotGeneration(policy)
		if e != nil {
			return e
		}
	}
	req.Policy = policy
	req.RuntimeSnapshot.PolicyGeneration = policy.Generation
	req.RuntimeSnapshot.DNSSelections = selections
	return nil
}
