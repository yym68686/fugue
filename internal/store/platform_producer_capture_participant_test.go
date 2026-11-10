package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func TestCaptureParticipantPrivatePostgresDynamicRosterAndCrash(t *testing.T) {
	open, crashRestart := captureDeferralPrivatePostgres(t)
	s, ctx, principal := open(), t.Context(), captureDeferralTestPrincipal()
	if _, err := s.db.ExecContext(ctx, producerCaptureDeferralSchema); err != nil {
		t.Fatal(err)
	}
	p := ProducerCaptureParticipant{ID: "boot-dynamic", Scopes: []string{platformproducer.Scope}}
	if _, err := s.AddProducerCaptureParticipantScopes(ctx, p, principal); err == nil {
		t.Fatal("scope extension created an unregistered boot")
	}
	if err := s.RegisterProducerCaptureParticipant(ctx, p, principal); err != nil {
		t.Fatal(err)
	}
	ordinary := model.Principal{ActorID: "ordinary"}
	if _, err := s.GetProducerCaptureParticipant(ctx, p.ID, ordinary); err == nil {
		t.Fatal("unprivileged participant read succeeded")
	}
	if _, err := s.AddProducerCaptureParticipantScopes(ctx, p, ordinary); err == nil {
		t.Fatal("unprivileged scope enrollment succeeded")
	}
	var originalEnrollment time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT enrolled_at FROM fugue_producer_capture_participants WHERE participant_id=$1`, p.ID).Scan(&originalEnrollment); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			scope := fmt.Sprintf("%s:zone-%d", platformproducer.Scope, i)
			got, err := s.AddProducerCaptureParticipantScopes(ctx, ProducerCaptureParticipant{ID: p.ID, Scopes: []string{scope}}, principal)
			if err != nil || !slices.Contains(got.Scopes, scope) {
				t.Errorf("concurrent enrollment: %v %v", got, err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	// Replay models a caller that cannot tell whether its previous commit
	// succeeded. It must not duplicate rows or renew the first enrollment.
	replayed, err := s.AddProducerCaptureParticipantScopes(ctx, ProducerCaptureParticipant{ID: p.ID, Scopes: []string{platformproducer.Scope + ":zone-0"}}, principal)
	if err != nil || len(replayed.Scopes) != 9 {
		t.Fatal("idempotent enrollment lost concurrent scopes", replayed, err)
	}
	var retainedEnrollment time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT enrolled_at FROM fugue_producer_capture_participants WHERE participant_id=$1 AND scope_key=$2`, p.ID, platformproducer.Scope).Scan(&retainedEnrollment); err != nil || !retainedEnrollment.Equal(originalEnrollment) {
		t.Fatal("extension renewed the original enrollment", err)
	}
	// A stale process cache cannot close a larger durable roster.
	if err := s.CloseProducerCaptureParticipant(ctx, p, principal); err == nil {
		t.Fatal("incomplete cached roster certified exit")
	}
	crashRestart()
	restarted := open()
	current, err := restarted.GetProducerCaptureParticipant(ctx, p.ID, principal)
	if err != nil || len(current.Scopes) != 9 {
		t.Fatal("committed roster lost across immediate PG restart", current, err)
	}
	if err := restarted.CloseProducerCaptureParticipant(ctx, current, principal); err != nil {
		t.Fatal(err)
	}
	crashRestart()
	final := open()
	if got, err := final.GetProducerCaptureParticipant(ctx, p.ID, principal); err != nil || !slices.Equal(got.Scopes, current.Scopes) {
		t.Fatal("closed roster tombstone lost", got, err)
	}
	if _, err := final.AddProducerCaptureParticipantScopes(ctx, ProducerCaptureParticipant{ID: p.ID, Scopes: []string{platformproducer.Scope + ":zone-next"}}, principal); err == nil {
		t.Fatal("closed boot reopened through scope extension")
	}
	if err := final.RegisterProducerCaptureParticipant(ctx, p, principal); err == nil {
		t.Fatal("closed boot reopened through registration")
	}
}

func TestCaptureParticipantPrivatePostgresScopeAndRetainedRowBounds(t *testing.T) {
	open, _ := captureDeferralPrivatePostgres(t)
	s, ctx, principal := open(), t.Context(), captureDeferralTestPrincipal()
	if _, err := s.db.ExecContext(ctx, producerCaptureDeferralSchema); err != nil {
		t.Fatal(err)
	}
	p := ProducerCaptureParticipant{ID: "boot-bounds", Scopes: []string{platformproducer.Scope}}
	if err := s.RegisterProducerCaptureParticipant(ctx, p, principal); err != nil {
		t.Fatal(err)
	}
	addition := ProducerCaptureParticipant{ID: p.ID}
	for i := range 31 {
		addition.Scopes = append(addition.Scopes, fmt.Sprintf("%s:zone-%d", platformproducer.Scope, i))
	}
	full, err := s.AddProducerCaptureParticipantScopes(ctx, addition, principal)
	if err != nil || len(full.Scopes) != 32 {
		t.Fatal("bounded scope roster refused", full, err)
	}
	if _, err := s.AddProducerCaptureParticipantScopes(ctx, ProducerCaptureParticipant{ID: p.ID, Scopes: []string{platformproducer.Scope + ":zone-overflow"}}, principal); err == nil {
		t.Fatal("33rd scope was admitted")
	}
	// Retained closed boots consume the same global budget; they cannot be
	// evicted to make a later enrollment appear possible.
	if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_producer_capture_participants(participant_id,scope_key,protocol,enrolled_at,closed_at) SELECT 'retained-'||g,$1,$2,clock_timestamp(),clock_timestamp() FROM generate_series(1,4063) g`, platformproducer.Scope, producerCaptureParticipantProtocol); err != nil {
		t.Fatal(err)
	}
	other := ProducerCaptureParticipant{ID: "boot-last-row", Scopes: []string{platformproducer.Scope}}
	if err := s.RegisterProducerCaptureParticipant(ctx, other, principal); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddProducerCaptureParticipantScopes(ctx, ProducerCaptureParticipant{ID: other.ID, Scopes: []string{platformproducer.Scope + ":zone-overflow"}}, principal); err == nil {
		t.Fatal("extension exceeded 4096 retained rows")
	}
	if _, err := s.AddProducerCaptureParticipantScopes(ctx, p, principal); err != nil {
		t.Fatal("idempotent read at the full row bound rewrote or refused", err)
	}
	got, err := s.GetProducerCaptureParticipant(ctx, other.ID, principal)
	if err != nil || len(got.Scopes) != 1 {
		t.Fatal("failed extension left a partial roster", got, err)
	}
}

func TestCaptureParticipantPrivatePostgresCloseSerializesWithExtension(t *testing.T) {
	open, _ := captureDeferralPrivatePostgres(t)
	s, ctx, principal := open(), t.Context(), captureDeferralTestPrincipal()
	if _, err := s.db.ExecContext(ctx, producerCaptureDeferralSchema); err != nil {
		t.Fatal(err)
	}
	p := ProducerCaptureParticipant{ID: "boot-exit-race", Scopes: []string{platformproducer.Scope}}
	if err := s.RegisterProducerCaptureParticipant(ctx, p, principal); err != nil {
		t.Fatal(err)
	}
	start, result := make(chan struct{}), make(chan error, 2)
	go func() {
		<-start
		_, err := s.AddProducerCaptureParticipantScopes(ctx, ProducerCaptureParticipant{ID: p.ID, Scopes: []string{platformproducer.Scope + ":zone-late"}}, principal)
		result <- err
	}()
	go func() { <-start; result <- s.CloseProducerCaptureParticipant(ctx, p, principal) }()
	close(start)
	first, second := <-result, <-result
	if first == nil && second == nil {
		t.Fatal("stale close and new scope both committed")
	}
	current, err := s.GetProducerCaptureParticipant(ctx, p.ID, principal)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CloseProducerCaptureParticipant(ctx, current, principal); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddProducerCaptureParticipantScopes(ctx, ProducerCaptureParticipant{ID: p.ID, Scopes: []string{platformproducer.Scope + ":zone-later"}}, principal); err == nil {
		t.Fatal("exit race reopened a closed boot")
	}
}
