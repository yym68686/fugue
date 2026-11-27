package participation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
)

type dispatchFixtureJournal struct {
	pendingHoldFixture
	reserveErr, openErr error
	reservations        int
}

func (f *dispatchFixtureJournal) ReserveExecution(_ context.Context, _, _, _ string) (productioncontrol.Claim, bool, error) {
	f.reservations++
	if f.reserveErr != nil {
		return productioncontrol.Claim{}, false, f.reserveErr
	}
	f.claimErr = nil
	f.head.Pending, f.head.LastFence = f.claim.IntentDigest, f.claim.Fence
	return f.claim, true, nil
}

func (f *dispatchFixtureJournal) VerifyOpenClaim(context.Context, productioncontrol.Claim) error {
	return f.openErr
}

type dispatchFixtureProvider struct {
	node                *corev1.Node
	boundary            string
	checks, writes      int
	drift, loseResponse bool
}

func (f *dispatchFixtureProvider) Check(_ context.Context, _ productioncontrol.VerifiedParticipationTopology, _ productioncontrol.Submission, head productioncontrol.ResourceState) (*corev1.Node, string, error) {
	f.checks++
	if head.Pending != "" {
		return nil, "", errors.New("preflight received an unresolved predecessor")
	}
	if f.drift && f.checks == 2 {
		return f.node, digest([]byte("new admission revisions")), nil
	}
	return f.node, f.boundary, nil
}

func (f *dispatchFixtureProvider) Write(_ context.Context, _ productioncontrol.Submission, _ *corev1.Node) (*corev1.Node, error) {
	f.writes++
	if f.loseResponse {
		return nil, errors.New("fixture lost connection after the request")
	}
	n := f.node.DeepCopy()
	n.ResourceVersion = "11"
	return n, nil
}

func signFixtureOfflineAuthority(t *testing.T, boundary string, now time.Time, readmission ...*OfflineHoldReadmission) (ed25519.PublicKey, []byte) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	statement := OfflineHoldAuthority{
		BoundaryDigest: boundary, EndpointAdmission: digest([]byte("fixture endpoint admission")),
		LegacyRevocation: digest([]byte("fixture revocation")), LegacyRequestDrain: digest([]byte("fixture drainage")),
		WriterIdentity: digest([]byte("fixture writer identity")), ObservedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(),
	}
	if len(readmission) == 1 {
		statement.Readmission = readmission[0]
	}
	message, err := statement.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	statement.Signature = ed25519.Sign(key, message)
	raw, err := json.Marshal(statement)
	if err != nil {
		t.Fatal(err)
	}
	return pub, raw
}

func TestOfflineDispatchNeverReplaysAfterAReservation(t *testing.T) {
	at := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	topology := offlineFixtureTopology(at)
	s, n, _ := draftFixtureOfflineHold(t, topology, at)
	intent, err := s.Intent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	scope := HoldResource("fixture", n.Name)
	claim := productioncontrol.Claim{Scope: scope, Fence: 1, IntentDigest: intent, ApprovalDigest: digest([]byte("approval"))}
	version := productioncontrol.Version{Generation: s.Intent.Generation, Digest: s.Intent.CurrentDigest}
	boundary := digest([]byte("fixture guarded boundary"))
	pub, signed := signFixtureOfflineAuthority(t, boundary, at)
	for _, tc := range []struct {
		name       string
		alter      func(*dispatchFixtureJournal, *dispatchFixtureProvider, *[]byte, *time.Time)
		wantClaim  bool
		wantChecks int
		wantWrites int
		wantErr    bool
	}{
		{name: "success", wantClaim: true, wantChecks: 2, wantWrites: 1},
		{name: "lost response", wantClaim: true, wantChecks: 2, wantWrites: 1, wantErr: true,
			alter: func(_ *dispatchFixtureJournal, p *dispatchFixtureProvider, _ *[]byte, _ *time.Time) {
				p.loseResponse = true
			}},
		{name: "guard drift after claim", wantClaim: true, wantChecks: 2, wantErr: true,
			alter: func(_ *dispatchFixtureJournal, p *dispatchFixtureProvider, _ *[]byte, _ *time.Time) { p.drift = true }},
		{name: "ownership lost after claim", wantClaim: true, wantChecks: 2, wantErr: true,
			alter: func(j *dispatchFixtureJournal, _ *dispatchFixtureProvider, _ *[]byte, _ *time.Time) {
				j.openErr = productioncontrol.ErrStaleFence
			}},
		{name: "uncertain commit", wantChecks: 1, wantErr: true,
			alter: func(j *dispatchFixtureJournal, _ *dispatchFixtureProvider, _ *[]byte, _ *time.Time) {
				j.reserveErr = errors.New("commit response lost")
			}},
		{name: "missing attestation", wantChecks: 1, wantErr: true,
			alter: func(_ *dispatchFixtureJournal, _ *dispatchFixtureProvider, signed *[]byte, _ *time.Time) {
				*signed = nil
			}},
		{name: "expired attestation", wantChecks: 1, wantErr: true,
			alter: func(_ *dispatchFixtureJournal, _ *dispatchFixtureProvider, _ *[]byte, now *time.Time) {
				*now = now.Add(time.Minute)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := &dispatchFixtureJournal{pendingHoldFixture: pendingHoldFixture{
				submission: s, topology: topology, claim: claim, claimErr: sql.ErrNoRows,
				head: productioncontrol.ResourceState{Baseline: productioncontrol.Baseline{Scope: scope}, LastVerified: version},
			}}
			p := &dispatchFixtureProvider{node: n, boundary: boundary}
			raw, now := signed, at
			if tc.alter != nil {
				tc.alter(j, p, &raw, &now)
			}
			run := func() (OfflineHoldDispatch, error) {
				return dispatchOfflineHold(context.Background(), j, p, "fixture", "guardian", intent, claim.ApprovalDigest, pub, raw, func() time.Time { return now })
			}
			out, err := run()
			if (err != nil) != tc.wantErr || p.writes != tc.wantWrites || p.checks != tc.wantChecks || out.Reserved != tc.wantClaim ||
				(out.ProviderResponseVersion != "") != (tc.wantWrites == 1 && !tc.wantErr) || j.head.LastVerified != version {
				t.Fatalf("dispatch=%+v err=%v checks=%d writes=%d", out, err, p.checks, p.writes)
			}
			if tc.wantClaim {
				p = &dispatchFixtureProvider{node: n, boundary: boundary} // New worker after interruption.
				replay, err := run()
				if err != nil || replay.Claim != claim || replay.Reserved || replay.ProviderResponseVersion != "" ||
					p.checks != 0 || p.writes != 0 || j.reservations != 1 {
					t.Fatalf("replay acquired new authority: %+v %v", replay, err)
				}
			}
		})
	}
}
