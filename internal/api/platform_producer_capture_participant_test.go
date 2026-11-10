package api

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

// The fake models durable occupancy and disconnected clients, not the SQL CAS.
// Real PostgreSQL tests separately exercise atomic registration/close/crashes.
type captureParticipantTestStore struct {
	mu               sync.Mutex
	registered       string
	registration     error
	works            map[string]store.ProducerCaptureWork
	finishes         int
	closeCalls       int
	failedFinish     bool
	admissionEntered chan struct{}
	admissionRelease chan struct{}
}

func (db *captureParticipantTestStore) RegisterProducerCaptureParticipant(_ context.Context, p store.ProducerCaptureParticipant, _ model.Principal) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.registration != nil {
		return db.registration
	}
	db.registered, db.works = p.ID, map[string]store.ProducerCaptureWork{}
	return nil
}

func (db *captureParticipantTestStore) BeginProducerCapture(_ context.Context, id, scope string, policy model.PlatformPublicationPrecondition, _ model.Principal) (store.ProducerCaptureWork, bool, error) {
	if db.admissionEntered != nil {
		close(db.admissionEntered)
		<-db.admissionRelease
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if id != db.registered {
		return store.ProducerCaptureWork{}, false, fmt.Errorf("not enrolled")
	}
	w := store.ProducerCaptureWork{ID: model.NewID("capture"), ParticipantID: id, Scope: scope, Policy: policy}
	db.works[w.ID] = w
	return w, false, nil
}

func TestCaptureParticipantCloseDoesNotWaitBehindAdmissionIO(t *testing.T) {
	db := &captureParticipantTestStore{admissionEntered: make(chan struct{}), admissionRelease: make(chan struct{})}
	p, err := newPlatformProducerCaptureParticipant(t.Context(), db, []string{platformproducer.Scope})
	if err != nil {
		t.Fatal(err)
	}
	type admission struct {
		finish func(context.Context) error
		err    error
	}
	result := make(chan admission, 1)
	go func() {
		finish, _, err := p.admit(t.Context(), platformproducer.Scope, model.PlatformPublicationPrecondition{})
		result <- admission{finish, err}
	}()
	<-db.admissionEntered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- p.close(ctx) }()
	select {
	case err := <-closed:
		if err == nil {
			t.Error("pending admission was declared joined")
		}
	case <-time.After(2 * time.Second):
		// Always release the fake I/O, even if a regression blocks the close.
		close(db.admissionRelease)
		<-result
		t.Fatal("canceled close blocked behind admission I/O")
	}
	close(db.admissionRelease)
	admitted := <-result
	if admitted.err != nil || admitted.finish == nil {
		t.Fatal("already reserved attempt was canceled by close", admitted.err)
	}
	if err := admitted.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (db *captureParticipantTestStore) FinishProducerCapture(_ context.Context, w store.ProducerCaptureWork, _ model.Principal) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.finishes++
	if db.failedFinish {
		return fmt.Errorf("uncertain finish")
	}
	delete(db.works, w.ID)
	return nil
}

func (db *captureParticipantTestStore) CloseProducerCaptureParticipant(_ context.Context, _ store.ProducerCaptureParticipant, _ model.Principal) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closeCalls++
	if len(db.works) != 0 {
		return fmt.Errorf("durable occupancy remains")
	}
	return nil
}

func TestCaptureParticipantJoinPreservesOccupiedWorkAndNoCancellation(t *testing.T) {
	for _, failedFinish := range []bool{false, true} {
		t.Run(fmt.Sprint(failedFinish), func(t *testing.T) {
			db := &captureParticipantTestStore{failedFinish: failedFinish}
			p, err := newPlatformProducerCaptureParticipant(t.Context(), db, []string{platformproducer.Scope})
			if err != nil {
				t.Fatal(err)
			}
			finishes := make(chan func(context.Context) error, 12)
			var wg sync.WaitGroup
			for range cap(finishes) {
				wg.Go(func() {
					finish, deferred, err := p.admit(t.Context(), platformproducer.Scope, model.PlatformPublicationPrecondition{})
					if err != nil || deferred || finish == nil {
						t.Errorf("admission: %t %v", deferred, err)
						return
					}
					finishes <- finish
				})
			}
			wg.Wait()
			close(finishes)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := p.close(ctx); err == nil {
				t.Fatal("close silently completed occupied work")
			}
			if db.closeCalls != 0 || len(db.works) != cap(finishes) {
				t.Fatal("close reached durable exit before local work joined")
			}
			if _, _, err := p.admit(t.Context(), platformproducer.Scope, model.PlatformPublicationPrecondition{}); err == nil {
				t.Fatal("closing process admitted later work")
			}
			for finish := range finishes {
				wg.Go(func() {
					if err := finish(t.Context()); (err != nil) != failedFinish {
						t.Errorf("finish: %v", err)
					}
					if err := finish(t.Context()); err == nil {
						t.Error("completion handle was consumed twice")
					}
				})
			}
			wg.Wait()
			if err := p.close(t.Context()); (err != nil) != failedFinish {
				t.Fatal("close did not preserve durable result", err)
			}
			if db.finishes != cap(finishes) || p.closed == failedFinish {
				t.Fatal("duplicate completion or fabricated participant close")
			}
		})
	}
}

func TestCaptureParticipantRefusesUncertainRegistrationAndLateInstall(t *testing.T) {
	db := &captureParticipantTestStore{registration: fmt.Errorf("uncertain registration")}
	if p, err := newPlatformProducerCaptureParticipant(t.Context(), db, []string{platformproducer.Scope}); err == nil || p != nil {
		t.Fatal("uncertain registration granted capture")
	}
	s := &Server{store: &store.Store{}}
	if err := s.configurePlatformProducerCaptureParticipant(t.Context(), []string{platformproducer.Scope}); err == nil || s.producerCaptureAdmission != nil || s.producerCaptureParticipant != nil {
		t.Fatal("uninstalled schema silently installed capture boundary")
	}
	s.markPlatformProducerCaptureStarted()
	if err := s.configurePlatformProducerCaptureParticipant(t.Context(), []string{platformproducer.Scope}); err == nil {
		t.Fatal("capture boundary retrofitted after work could have started")
	}
}
