package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

const buildMemoryRetention = 30 * 24 * time.Hour
const buildMemorySampleLimit = 8
const buildMemoryProfileLimit = 16

type buildMemorySample struct {
	PodUID     string    `json:"pod_uid"`
	PeakBytes  int64     `json:"peak_bytes"`
	ObservedAt time.Time `json:"observed_at"`
}

type buildMemoryLedger struct {
	Version  int                            `json:"version"`
	Profiles map[string][]buildMemorySample `json:"profiles"`
}

func buildMemoryLedgerKey(app model.App) string {
	return "builder-memory/v1/" + app.TenantID + "/" + app.ID
}

func buildMemoryScope(source model.AppSource, spec model.AppSpec) string {
	// Ordinary commits and uploads reuse history, but different source roots,
	// repository branches, configuration, tenants and apps do not share it.
	data, _ := json.Marshal(struct {
		Source   model.AppSource
		Env      map[string]string
		Files    []model.AppFile
		Stateful bool
	}{model.AppSource{
		Type: source.Type, RepoURL: source.RepoURL, RepoBranch: source.RepoBranch,
		SourceDir: source.SourceDir, BuildStrategy: source.BuildStrategy,
		DockerfilePath: source.DockerfilePath, BuildContextDir: source.BuildContextDir,
		ComposeService: source.ComposeService,
	}, spec.Env, spec.Files, spec.Workspace != nil || spec.PersistentStorage != nil || spec.Postgres != nil})
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func buildMemoryProfile(scope, profile string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(scope+"\x00"+profile)))
}

func decodeBuildMemoryLedger(raw []byte, now time.Time) (buildMemoryLedger, error) {
	ledger := buildMemoryLedger{Version: 1, Profiles: map[string][]buildMemorySample{}}
	if len(raw) == 0 {
		return ledger, nil
	}
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return ledger, err
	}
	if ledger.Version != 1 || ledger.Profiles == nil || len(ledger.Profiles) > buildMemoryProfileLimit {
		return ledger, errors.New("invalid build memory history")
	}
	for key, samples := range ledger.Profiles {
		if len(key) != 64 || len(samples) > buildMemorySampleLimit {
			return ledger, errors.New("invalid build memory profile")
		}
		retained := make([]buildMemorySample, 0, len(samples))
		for _, sample := range samples {
			if sample.PodUID == "" || len(sample.PodUID) > 128 || sample.PeakBytes <= 0 || sample.PeakBytes > sourceimport.MaxBuilderHeavyMemoryLimitBytes() || sample.ObservedAt.IsZero() {
				return ledger, errors.New("invalid build memory observation")
			}
			if !sample.ObservedAt.Before(now.Add(-buildMemoryRetention)) && !sample.ObservedAt.After(now.Add(time.Minute)) {
				retained = append(retained, sample)
			}
		}
		if len(retained) == 0 {
			delete(ledger.Profiles, key)
		} else {
			ledger.Profiles[key] = retained
		}
	}
	return ledger, nil
}

func (s *Service) readBuildMemoryTarget(ctx context.Context, ledgerKey, profile string) (int64, error) {
	raw, err := s.Store.ReadObservationCheckpoint(ctx, ledgerKey)
	if err != nil {
		return 0, err
	}
	ledger, err := decodeBuildMemoryLedger(raw, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	var peak int64
	for _, sample := range ledger.Profiles[profile] {
		if sample.PeakBytes > peak {
			peak = sample.PeakBytes
		}
	}
	if peak == 0 {
		return 0, nil
	}
	// A recent maximum plus headroom tolerates cold caches and ordinary changes.
	// This is a prediction only; allocation still obeys policy and OOM admission.
	const mib = int64(1 << 20)
	target := peak + (peak*30+99)/100 + 128*mib
	target = (target + mib - 1) / mib * mib
	return min(target, sourceimport.MaxBuilderHeavyMemoryLimitBytes()), nil
}

func (s *Service) recordBuildMemoryPeak(ctx context.Context, ledgerKey, profile, uid string, peak int64) error {
	if len(profile) != 64 || uid == "" || len(uid) > 128 || peak <= 0 || peak > sourceimport.MaxBuilderHeavyMemoryLimitBytes() {
		return errors.New("invalid build memory observation")
	}
	for {
		written, err := s.Store.UpdateObservationCheckpoint(ctx, ledgerKey, func(raw []byte) ([]byte, error) {
			now := time.Now().UTC()
			ledger, err := decodeBuildMemoryLedger(raw, now)
			if err != nil {
				return nil, err
			}
			samples := ledger.Profiles[profile]
			for _, sample := range samples {
				if sample.PodUID == uid {
					return nil, nil
				}
			}
			if len(samples) == 0 && len(ledger.Profiles) >= buildMemoryProfileLimit {
				oldestKey := ""
				var oldest time.Time
				for key, retained := range ledger.Profiles {
					latest := retained[len(retained)-1].ObservedAt
					if oldestKey == "" || latest.Before(oldest) || latest.Equal(oldest) && key < oldestKey {
						oldestKey, oldest = key, latest
					}
				}
				delete(ledger.Profiles, oldestKey)
			}
			samples = append(samples, buildMemorySample{uid, peak, now})
			ledger.Profiles[profile] = samples[max(0, len(samples)-buildMemorySampleLimit):]
			return json.Marshal(ledger)
		})
		if err != nil || written {
			return err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
