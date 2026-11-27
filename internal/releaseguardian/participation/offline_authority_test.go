package participation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
)

func offlineFixtureTopology(now time.Time) productioncontrol.VerifiedParticipationTopology {
	adopt := false
	return productioncontrol.VerifiedParticipationTopology{
		Version:        productioncontrol.Version{Generation: 1, Digest: digest([]byte("topology"))},
		IntentDigest:   digest([]byte("topology intent")),
		ApprovalDigest: digest([]byte("approval")), RecordedAt: now.Add(-time.Minute),
		Policy: productioncontrol.ParticipationPolicy{
			Schema: productioncontrol.BoundParticipationPolicySchema, Generation: 1, Namespace: "fixture",
			Origin: "https://fugue.example.test", Readers: productioncontrol.ParticipationReaders{Guardian: "guardian", CanaryProber: "canary"},
			Kubernetes: &productioncontrol.ParticipationKubernetesBinding{
				Endpoint: "https://cluster.example.test", CADigest: digest([]byte("ca")), NamespaceUID: "ns-a",
			},
			Members: []productioncontrol.ParticipationMember{{
				Node: "node-a", NodeUID: "uid-a", Group: "cell-a", Component: "component-a",
				Front: "front-a", Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"},
				Reason: "network_unavailable", AdoptHold: &adopt, StableSeconds: 300,
				Probes: []productioncontrol.ParticipationProbe{{Host: "front-a.example.test", Path: "/"}, {Host: "front-b.example.test", Path: "/health"}},
			}},
		},
	}
}

func TestOfflineHoldAuthorityRequiresExactSignedCutover(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	topology := offlineFixtureTopology(now)
	s, _, _ := draftFixtureOfflineHold(t, topology, now)
	const writer = "system:serviceaccount:fixture:writer-a"
	p, b, err := RenderOfflineHoldGuard(s, writer)
	if err != nil {
		t.Fatal(err)
	}
	guard := FreezeObservation{
		Node: "node-a", Policy: ObjectVersion{Name: p.Name, UID: "policy-a", ResourceVersion: "100"},
		Binding: ObjectVersion{Name: b.Name, UID: "binding-a", ResourceVersion: "110"}, CurrentTypeChecked: true,
	}
	boundary, err := OfflineHoldAuthorityBoundary(topology, s, writer, guard)
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := OfflineHoldAuthority{
		BoundaryDigest: boundary, EndpointAdmission: digest([]byte("endpoints")),
		LegacyRevocation: digest([]byte("revocation")), LegacyRequestDrain: digest([]byte("drainage")),
		WriterIdentity: digest([]byte("writer identity")), ObservedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(),
	}
	signing, err := a.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	a.Signature = ed25519.Sign(key, signing)
	encode := func(a OfflineHoldAuthority) []byte {
		t.Helper()
		raw, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	raw := encode(a)
	if err := verifyOfflineHoldAuthority(raw, pub, boundary, now); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*productioncontrol.VerifiedParticipationTopology, *productioncontrol.Submission, *string, *FreezeObservation){
		"another request": func(_ *productioncontrol.VerifiedParticipationTopology, s *productioncontrol.Submission, _ *string, _ *FreezeObservation) {
			s.Intent.RequestID = "request-other"
		},
		"another approval": func(p *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.Submission, _ *string, _ *FreezeObservation) {
			p.ApprovalDigest = digest([]byte("another approval"))
		},
		"another Kubernetes endpoint": func(p *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.Submission, _ *string, _ *FreezeObservation) {
			p.Policy.Kubernetes.Endpoint = "https://other.example.test"
		},
		"another writer": func(_ *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.Submission, w *string, _ *FreezeObservation) {
			*w = "system:serviceaccount:fixture:writer-b"
		},
		"another policy revision": func(_ *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.Submission, _ *string, g *FreezeObservation) {
			g.Policy.ResourceVersion = "101"
		},
		"recreated binding": func(_ *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.Submission, _ *string, g *FreezeObservation) {
			g.Binding.UID = "binding-b"
		},
		"unchecked guard": func(_ *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.Submission, _ *string, g *FreezeObservation) {
			g.CurrentTypeChecked = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, submission, w, g := topology, s, writer, guard
			kube := *p.Policy.Kubernetes
			p.Policy.Kubernetes = &kube
			change(&p, &submission, &w, &g)
			if changed, err := OfflineHoldAuthorityBoundary(p, submission, w, g); err == nil &&
				verifyOfflineHoldAuthority(raw, pub, changed, now) == nil {
				t.Fatal("provider authority accepted a different boundary")
			}
		})
	}
	for _, at := range []time.Time{now.Add(-time.Second), now.Add(time.Minute)} {
		if err := verifyOfflineHoldAuthority(raw, pub, boundary, at); err == nil {
			t.Fatal("provider authority accepted outside its freshness window")
		}
	}
	for _, d := range []string{"", a.LegacyRevocation, digest([]byte("tampered drainage"))} {
		changed := a
		changed.LegacyRequestDrain = d
		if err := verifyOfflineHoldAuthority(encode(changed), pub, boundary, now); err == nil {
			t.Fatal("missing, reused, or tampered drainage accepted")
		}
	}
	for _, altered := range [][]byte{
		append(bytes.Clone(raw), '\n'),
		append([]byte(`{"observedAtUnix":0,`), raw[1:]...),
	} {
		if err := verifyOfflineHoldAuthority(altered, pub, boundary, now); err == nil {
			t.Fatal("non-canonical authority statement accepted")
		}
	}
}
