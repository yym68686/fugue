package api

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestOrdinaryHostedBaselineDoesNotFollowItsOwnParent(t *testing.T) {
	projection, _, _, now := physicalQueryProjectionFixture(t)
	req := platformconfig.CompileRequest{Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot}
	key, err := isolatedBaselineDigest(req, "policy", "compiler", nil)
	if err != nil {
		t.Fatal(err)
	}
	first := hostedCapturePlan{phase: "external", managed: []string{"managed.example.test"}, external: []string{"external.example.test"}, selected: model.PlatformArtifactRelease{ID: "release-1", ArtifactID: "full-1"}}
	next := first
	next.selected.ID, next.selected.ArtifactID = "release-2", "full-2"
	a, _ := first.sourceDigest("business")
	b, _ := next.sourceDigest("business")
	if a == b || !first.ordinaryPathOffers() || !next.ordinaryPathOffers() {
		t.Fatal("lost parent binding or domain retry")
	}
	candidate := model.PlatformArtifact{ID: next.selected.ArtifactID, Metadata: map[string]string{isolatedBaselineInput: key}}
	same := func(p hostedCapturePlan, input string, owned bool, age, remaining time.Duration) bool {
		return sameHostedBaseline(p, input, candidate, owned, now.Add(-age), now.Add(remaining), now, time.Minute, time.Hour)
	}
	if !same(next, key, true, time.Second, time.Hour) || same(first, key, true, time.Second, time.Hour) || same(next, key, false, time.Second, time.Hour) || same(next, key, true, time.Hour, time.Hour) || same(next, key, true, time.Second, time.Minute) {
		t.Fatal("ordinary gate must use current owned full with time to refresh")
	}
	other := req
	other.Intent.DNS = append([]platformconfig.DNSIntent(nil), req.Intent.DNS...)
	other.Intent.DNS[0].TTL++
	for _, v := range []struct {
		r     platformconfig.CompileRequest
		a, c  string
		holds []platformconfig.HostedAddressHold
	}{
		{other, "policy", "compiler", nil}, {req, "new-policy", "compiler", nil}, {req, "policy", "new-compiler", nil},
		{req, "policy", "compiler", []platformconfig.HostedAddressHold{{EdgeID: "edge-a", EdgeGroupID: "group", Address: "192.0.2.1"}}},
	} {
		nextKey, e := isolatedBaselineDigest(v.r, v.a, v.c, v.holds)
		if e != nil || nextKey == key || same(next, nextKey, true, time.Second, time.Hour) {
			t.Fatal("suppressed unrelated change or new hold", e)
		}
	}
}
