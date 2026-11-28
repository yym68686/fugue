package controller

import (
	"context"
	"io"
	"time"

	"fugue/internal/appmail"
	"fugue/internal/observability"
)

type appMailSchedulerObservation struct {
	Seeded       bool
	Provider     bool
	WorkerOwner  bool
	Checked      time.Time
	ProviderRead time.Time
}

func (s *Service) runAppMailScheduler(ctx context.Context) {
	defer s.appMailObservation.Store(nil)
	provider := appmail.NewProvider(s.Store)
	reconciler, err := appmail.NewLeasedReconciler(s.Store, provider, s.Config.AppBaseDomain)
	if err != nil {
		return
	}
	var validateAt, reportAt, probeAt, providerRead time.Time
	var providerReady bool
	for ctx.Err() == nil {
		check, cancel := context.WithTimeout(ctx, 3*time.Second)
		control, prepareErr := s.Store.PrepareAppMailReconciliation(check)
		cancel()
		delay := 5 * time.Second
		if prepareErr == nil && control.Seeded {
			if now := time.Now(); !now.Before(validateAt) {
				providerReady = provider.CheckReconciliationConfig(s.Config.AppBaseDomain) == nil
				validateAt = time.Now().Add(15 * time.Second)
			}
		} else {
			providerReady = false
			validateAt = time.Time{}
		}
		if !providerReady || control.Owner != "api" {
			probeAt, providerRead = time.Time{}, time.Time{}
		} else if now := time.Now(); !now.Before(probeAt) {
			// Standby gets one authenticated, exact-generation GET from its
			// own network path per minute; it never invokes provider PUT.
			providerRead = time.Time{}
			probeAt = now.Add(time.Minute)
			probe, stop := context.WithTimeout(ctx, 5*time.Second)
			if witness, readErr := s.Store.AppMailStandbyWitness(probe); readErr == nil {
				if _, readErr = provider.ObserveBinding(probe, witness); readErr == nil {
					providerRead = time.Now()
				}
			}
			stop()
		}
		if ctx.Err() != nil {
			return
		}
		s.appMailObservation.Store(&appMailSchedulerObservation{
			Seeded: control.Seeded, Provider: providerReady,
			WorkerOwner: control.Owner == "controller", Checked: time.Now(), ProviderRead: providerRead,
		})
		if prepareErr == nil && !control.Seeded {
			delay = time.Second
		}
		if prepareErr == nil && control.Seeded && providerReady && control.Owner == "controller" {
			claimed, runErr := reconciler.RunOnce(ctx)
			switch {
			case runErr != nil:
				if now := time.Now(); ctx.Err() == nil && !now.Before(reportAt) {
					s.Logger.Printf("app mail reconciliation attempt failed; pending work remains in the database")
					reportAt = now.Add(30 * time.Second)
				}
				delay = time.Second
			case claimed:
				delay = 200 * time.Millisecond
			default:
				delay = time.Second
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Service) writeAppMailSchedulerMetrics(w io.Writer) {
	o := s.appMailObservation.Load()
	if o == nil {
		o = &appMailSchedulerObservation{}
	}
	observability.WriteGaugeMetric(w, "fugue_controller_app_mail_seed_complete", "Whether the durable application-mail schedule covers existing bindings.", nil, boolGauge(o.Seeded))
	observability.WriteGaugeMetric(w, "fugue_controller_app_mail_provider_config_ready", "Whether the local application-mail provider configuration is valid; no request is sent by this check.", nil, boolGauge(o.Provider))
	observability.WriteGaugeMetric(w, "fugue_controller_app_mail_executor_owner", "Whether the database designates the controller as the application-mail writer.", nil, boolGauge(o.WorkerOwner))
	observability.WriteGaugeMetric(w, "fugue_controller_app_mail_last_check_timestamp_seconds", "Unix timestamp of the latest application-mail scheduler gate check on this process.", nil, appMailMetricTimestamp(o.Checked))
	observability.WriteGaugeMetric(w, "fugue_controller_app_mail_provider_read_timestamp_seconds", "Unix timestamp of a fresh authenticated exact-generation provider GET while in standby; zero when unavailable.", nil, appMailMetricTimestamp(o.ProviderRead))
}

func appMailMetricTimestamp(at time.Time) float64 {
	if at.IsZero() {
		return 0
	}
	return float64(at.Unix())
}
