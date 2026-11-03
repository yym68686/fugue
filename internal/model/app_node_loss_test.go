package model

import "testing"

func TestAppNodeLossPolicyValidationAndLegacyDefaults(t *testing.T) {
	seconds := int64(0)
	for _, policy := range []*AppNodeLossPolicy{nil, {Mode: AppNodeLossWait}, {Mode: AppNodeLossLegacy}, {Mode: AppNodeLossReplace}, {Mode: AppNodeLossReplace, TolerationSeconds: &seconds}} {
		if err := ValidateAppNodeLossPolicy(policy); err != nil {
			t.Fatal(err)
		}
	}
	for _, policy := range []*AppNodeLossPolicy{{}, {Mode: "automatic"}, {Mode: AppNodeLossWait, TolerationSeconds: &seconds}, {Mode: AppNodeLossLegacy, TolerationSeconds: &seconds}} {
		if ValidateAppNodeLossPolicy(policy) == nil {
			t.Fatal("invalid policy accepted", policy)
		}
	}
	seconds = -1
	if ValidateAppNodeLossPolicy(&AppNodeLossPolicy{Mode: AppNodeLossReplace, TolerationSeconds: &seconds}) == nil {
		t.Fatal("negative timeout accepted")
	}
	seconds = 86401
	if ValidateAppNodeLossPolicy(&AppNodeLossPolicy{Mode: AppNodeLossReplace, TolerationSeconds: &seconds}) == nil {
		t.Fatal("excessive timeout accepted")
	}
	var legacy AppSpec
	ApplyAppSpecDefaults(&legacy)
	if legacy.NodeLoss != nil {
		t.Fatal("reading legacy spec silently changed lifecycle")
	}
}
