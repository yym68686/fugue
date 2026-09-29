package platformcontrol

import "testing"

func TestConsumerScopeRequiresExplicitExactCell(t *testing.T) {
	for _, tc := range []struct {
		scope, group string
		valid        bool
	}{
		{"", "cell-a", true}, {"global", "cell-a", true}, {"global", "edge-group-old", true},
		{"authority-cell:cell-a", "cell-a", true}, {"authority-cell:cell-b", "cell-a", false},
		{"authority-cell:cell-a", "edge-group-old", false}, {"authority-cell:cell-a", "", false},
		{"tenant:other", "cell-a", false}, {" global", "cell-a", false},
	} {
		if (ValidateConsumerScope(tc.scope, tc.group) == nil) != tc.valid {
			t.Fatal(tc)
		}
	}
	if ConfiguredConsumerScope("") != "global" {
		t.Fatal("compatibility scope changed implicitly")
	}
}
