package controller

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"

	"github.com/jackc/pgx/v5"
)

var errImageMigrationEndpointIdentity = errors.New("stable database Service identity is unavailable")

type postgresImageEndpointSampler struct {
	cancel   context.CancelFunc
	done     chan struct{}
	once     sync.Once
	begin    chan struct{}
	start    sync.Once
	record   func(postgresImageEndpointTiming)
	timing   postgresImageEndpointTiming
	baseline func()
}

func (s *postgresImageEndpointSampler) MarkCutover() {
	s.start.Do(func() {
		if s.baseline != nil {
			s.baseline()
		}
		close(s.begin)
	})
}

type postgresImageEndpointTiming struct {
	samples           int
	lastSuccess       time.Time
	lastBeforeFailure time.Time
	firstFailure      time.Time
	lastFailure       time.Time
	firstRecovery     time.Time
	unavailableReason string
}

func (t *postgresImageEndpointTiming) add(at time.Time, success bool) {
	t.samples++
	switch {
	case t.firstFailure.IsZero() && success:
		t.lastSuccess = at
	case t.firstFailure.IsZero():
		t.lastBeforeFailure, t.firstFailure, t.lastFailure = t.lastSuccess, at, at
	case t.firstRecovery.IsZero() && success:
		t.firstRecovery = at
	case t.firstRecovery.IsZero():
		t.lastFailure = at
	}
}

func (s *postgresImageEndpointSampler) StopAndRecord() {
	s.once.Do(func() {
		s.cancel()
		<-s.done
		s.record(s.timing)
	})
}

func (s *Service) observeImageEndpoint(ctx context.Context, op model.Operation, c *kubeClient, w postgresImageWitness, postgres model.AppPostgresSpec, resuming bool) *postgresImageEndpointSampler {
	ctx, cancel := context.WithCancel(ctx)
	sampler := &postgresImageEndpointSampler{cancel: cancel, done: make(chan struct{}), begin: make(chan struct{}), record: func(t postgresImageEndpointTiming) {
		status := "not_observed"
		fields := map[string]any{"samples": t.samples, "probe_interval_ms": 1000}
		if !t.lastBeforeFailure.IsZero() {
			fields["last_success_before_failure"] = t.lastBeforeFailure
		}
		if !t.firstFailure.IsZero() {
			fields["first_failed_probe"], fields["last_failed_probe"] = t.firstFailure, t.lastFailure
			status = "incomplete"
		}
		if !t.firstRecovery.IsZero() {
			fields["first_recovered_probe"] = t.firstRecovery
			if !t.lastBeforeFailure.IsZero() {
				fields["observed_failure_to_recovery_ms"] = t.firstRecovery.Sub(t.firstFailure).Milliseconds()
				status = "sampled"
			}
		}
		if t.unavailableReason != "" {
			fields["reason"] = t.unavailableReason
		}
		fields["status"] = status
		_, _ = s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: op.TenantID, AppID: op.AppID, OperationID: op.ID,
			Type: pgimage.EndpointTimingEvidence, Summary: "Stable database Service SQL probe: " + status, PayloadVersion: 1, Payload: fields})
	}}
	if resuming {
		sampler.timing.unavailableReason = "controller resumed after an uncertain write; original probe window is unavailable"
		close(sampler.done)
		return sampler
	}
	// Resolve the stable endpoint before any Cluster mutation. Take a fresh
	// baseline at cutover, after waiting for the temporary policy to load.
	endpoint, err := imageMigrationEndpoint(ctx, c, w, postgres)
	if err != nil {
		sampler.timing.unavailableReason = "stable Service identity or address could not be verified"
		close(sampler.done)
		return sampler
	}
	sampler.baseline = func() {
		if !probeImageMigrationEndpoint(ctx, endpoint) {
			sampler.timing.unavailableReason = "stable Service SQL probe did not pass before image replacement"
			return
		}
		sampler.timing.add(time.Now().UTC(), true)
	}
	go func() {
		defer close(sampler.done)
		select {
		case <-ctx.Done():
			return
		case <-sampler.begin:
		}
		if sampler.timing.lastSuccess.IsZero() {
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for sampler.timing.firstRecovery.IsZero() {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok := probeImageMigrationEndpoint(ctx, endpoint)
				if ctx.Err() == nil {
					sampler.timing.add(time.Now().UTC(), ok)
				}
			}
		}
	}()
	return sampler
}

func imageMigrationEndpoint(ctx context.Context, c *kubeClient, w postgresImageWitness, postgres model.AppPostgresSpec) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	service, err := c.imageMigrationObject(ctx, "/api/v1/namespaces/"+url.PathEscape(w.Namespace)+"/services/"+url.PathEscape(model.PostgresEndpointName(postgres)))
	if err != nil {
		return "", err
	}
	spec := imageObject(service["spec"])
	selector := imageObject(spec["selector"])
	address := imageString(spec, "clusterIP")
	if net.ParseIP(address) == nil || imageString(imageObject(service["metadata"]), "uid") == "" ||
		len(selector) != 2 || selector[cloudNativePGClusterLabel] != w.Cluster || selector[cloudNativePGInstanceRoleLabel] != "primary" {
		return "", errImageMigrationEndpointIdentity
	}
	// This is the same cluster Service VIP as the consumer's DNS name.
	// Require encryption, and never emit the URL or its credentials.
	u, err := url.Parse(managedPostgresResizeDatabaseURL(address, postgres))
	if err != nil {
		return "", errImageMigrationEndpointIdentity
	}
	q := u.Query()
	q.Set("sslmode", "require")
	q.Set("application_name", "fugue-postgres-image-migration-probe")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func probeImageMigrationEndpoint(ctx context.Context, endpoint string) bool {
	probe, cancel := context.WithTimeout(ctx, 850*time.Millisecond)
	defer cancel()
	conn, err := pgx.Connect(probe, endpoint)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close(probe) }()
	var writable bool
	return conn.QueryRow(probe, "SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only') = 'off'").Scan(&writable) == nil && writable
}
