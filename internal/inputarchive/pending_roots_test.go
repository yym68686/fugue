package inputarchive

import (
	"strings"
	"testing"
)

func TestPendingRootCanonicalAndExactReceipt(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	r := PendingRootRegistration{Schema: PendingRootSchema, Owner: "authority/neutral", Generation: "generation-neutral", SourceDigest: digest,
		References: []HotReference{{Kind: "artifact", ID: "neutral", ExpectedHash: digest}, {Kind: "input", ID: digest, ExpectedHash: digest}}}
	raw, hash, err := r.Canonical()
	if err != nil || len(raw) == 0 || len(hash) != 64 {
		t.Fatal(hash, err)
	}
	receipt := PendingRootReceipt{hash, 1, 0}
	if !receipt.Matches(r) {
		t.Fatal("exact append receipt rejected")
	}
	for _, change := range []func(*PendingRootRegistration){
		func(r *PendingRootRegistration) { r.Schema = "unknown" },
		func(r *PendingRootRegistration) { r.Owner = " other " },
		func(r *PendingRootRegistration) { r.Generation += "\x00" },
		func(r *PendingRootRegistration) { r.SourceDigest = strings.Repeat("a", 64) },
		func(r *PendingRootRegistration) { r.References = nil },
		func(r *PendingRootRegistration) { r.References = append(r.References, r.References[0]) },
		func(r *PendingRootRegistration) { r.References[0].ExpectedHash = strings.Repeat("a", 64) },
		func(r *PendingRootRegistration) { r.References[0].ArtifactKind = "claimed" },
		func(r *PendingRootRegistration) { r.References[0].Kind = "generation" },
		func(r *PendingRootRegistration) { r.References[1].ExpectedHash = "sha256:" + strings.Repeat("b", 64) },
	} {
		copy := r
		copy.References = append([]HotReference(nil), r.References...)
		change(&copy)
		if _, _, err := copy.Canonical(); err == nil || receipt.Matches(copy) {
			t.Fatal("invalid or mismatched registration accepted", copy)
		}
	}
	for _, bad := range []PendingRootReceipt{{hash, 0, 0}, {hash, 1, -1}, {strings.Repeat("b", 64), 1, 0}} {
		if bad.Matches(r) {
			t.Fatal("unbound receipt accepted", bad)
		}
	}
}
