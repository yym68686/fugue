package inputarchive

import (
	"bytes"
	"strings"
	"testing"
)

func TestPendingRootAuthorityPolicyRejectsAmbiguousOrPartialEncoding(t *testing.T) {
	a := PendingRootAuthority{Owner: "authority/neutral", Namespace: "neutral", ServiceAccount: "neutral", ServiceAccountUID: "sa", Deployment: "neutral", DeploymentUID: "deployment", PodUID: "pod", SourceSHA: strings.Repeat("a", 40), ImageRef: "registry.test/neutral@sha256:" + strings.Repeat("b", 64), PolicyRevision: "policy-1"}
	p := PendingRootAuthorityPolicy{Schema: PendingRootAuthorityPolicySchema, Leases: []PendingRootAuthorityLease{{Authority: a, LeaseID: strings.Repeat("c", 64), ExpiresUnix: 1}}}
	raw, err := p.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodePendingRootAuthorityPolicy(raw); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{append([]byte(" "), raw...), append([]byte(`{"schema":"other",`), raw[1:]...), append([]byte(`{"unknown":true,`), raw[1:]...), bytes.Replace(raw, []byte(`"PodUID":"pod"`), []byte(`"PodUID":"pod","poduid":"replacement"`), 1), append(raw, []byte(`{}`)...)} {
		if _, err = DecodePendingRootAuthorityPolicy(bad); err == nil {
			t.Fatal("ambiguous policy accepted")
		}
	}
	p.Leases = append(p.Leases, p.Leases[0])
	if _, err = p.Canonical(); err == nil {
		t.Fatal("duplicate lease owner/Pod")
	}
	p.Leases = []PendingRootAuthorityLease{}
	if _, err = p.Canonical(); err != nil {
		t.Fatal("explicit empty policy cannot revoke", err)
	}
	p.Leases = nil
	if _, err = p.Canonical(); err == nil {
		t.Fatal("null policy became complete empty registry")
	}
}
