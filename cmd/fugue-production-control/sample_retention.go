package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"fugue/internal/productioncontrol"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type sampleRetentionObserverSettings struct {
	Provider productioncontrol.SampleRetentionProvider `json:"provider"`
	Database string                                    `json:"databaseUrl"`
}

func openSampleRetentionObserver(path string) (productioncontrol.SampleRetentionProvider, *sql.DB, error) {
	var settings sampleRetentionObserverSettings
	raw, err := readFile(path, 16<<10, true)
	if err != nil {
		return settings.Provider, nil, err
	}
	defer clear(raw)
	if decodeSampleRetentionConfig(raw, &settings) != nil || settings.Database == "" {
		return settings.Provider, nil, errors.New("invalid private sample retention observer configuration")
	}
	cfg, err := pgx.ParseConfig(settings.Database)
	if err != nil || cfg.Host == "" || cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify ||
		cfg.TLSConfig.ServerName != cfg.Host || len(cfg.Fallbacks) != 0 ||
		cfg.Database != settings.Provider.Database || cfg.User != settings.Provider.Role {
		return settings.Provider, nil, errors.New("sample retention observer requires the exact database and role on one hostname-verified PostgreSQL endpoint")
	}
	cfg.ConnectTimeout = 5 * time.Second
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(1)
	return settings.Provider, db, nil
}

func observeSampleRetention(ctx context.Context, path string, out io.Writer) error {
	provider, db, err := openSampleRetentionObserver(path)
	if err != nil {
		return err
	}
	defer db.Close()
	observation, err := provider.Observe(ctx, db)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(observation)
}

func decodeSampleRetentionConfig(raw []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid sample retention configuration")
	}
	return nil
}

var sampleRetentionWitnessDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type sampleRetentionWitnessDirectory struct{ root *os.Root }

func (s sampleRetentionWitnessDirectory) ReadExecutionWitness(ctx context.Context, digest string) ([]byte, error) {
	if !sampleRetentionWitnessDigest.MatchString(digest) || ctx.Err() != nil {
		return nil, errors.New("invalid sample retention witness read")
	}
	name := digest[7:] + ".json"
	info, err := s.root.Lstat(name)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("sample retention witness file is unavailable")
	}
	f, err := s.root.Open(name)
	if err != nil {
		return nil, errors.New("sample retention witness file could not be opened")
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, productioncontrol.MaxWitnessBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > productioncontrol.MaxWitnessBytes {
		return nil, errors.New("invalid sample retention witness file")
	}
	return raw, nil
}

func runSampleRetentionBaseline(ctx context.Context, path, evidence string, out io.Writer, now func() time.Time) error {
	var settings struct {
		JournalFile              string            `json:"journalFile"`
		ObserverFile             string            `json:"observerFile"`
		WitnessDirectory         string            `json:"witnessDirectory"`
		ObserverKeyID            string            `json:"observerKeyId"`
		ObserverPublicKey        ed25519.PublicKey `json:"observerPublicKey"`
		LegacyRevocationEvidence string            `json:"legacyRevocationEvidence"`
	}
	raw, err := readFile(path, 16<<10, true)
	if err != nil {
		return err
	}
	if decodeSampleRetentionConfig(raw, &settings) != nil || !filepath.IsAbs(settings.ObserverFile) ||
		!filepath.IsAbs(settings.JournalFile) || !sampleRetentionWitnessDigest.MatchString(settings.LegacyRevocationEvidence) ||
		(evidence != "" && (!sampleRetentionWitnessDigest.MatchString(evidence) || !filepath.IsAbs(settings.WitnessDirectory))) {
		return errors.New("sample retention baseline requires a dedicated observer and reviewed legacy revocation evidence")
	}
	p, db, err := openSampleRetentionObserver(settings.ObserverFile)
	if err != nil {
		return err
	}
	defer db.Close()
	return withJournalDeadline(ctx, settings.JournalFile, evidence == "", 20*time.Second,
		func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
			if err := j.VerifyIndependentObserverKey(settings.ObserverPublicKey); err != nil {
				return err
			}
			scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: productioncontrol.ObservationSampleRetentionResource}
			if evidence == "" {
				observed, err := p.Observe(ctx, db)
				if err != nil {
					return err
				}
				draft, err := productioncontrol.DraftSampleRetentionBaseline(scope, observed, settings.LegacyRevocationEvidence, settings.ObserverKeyID, now())
				if err != nil {
					return err
				}
				return json.NewEncoder(out).Encode(draft)
			}
			root, err := os.OpenRoot(settings.WitnessDirectory)
			if err != nil {
				return errors.New("independent sample retention witness directory is unavailable")
			}
			defer root.Close()
			source := sampleRetentionWitnessDirectory{root}
			raw, err := source.ReadExecutionWitness(ctx, evidence)
			if err != nil {
				return err
			}
			var w productioncontrol.ExecutionWitness
			if err := decodeSampleRetentionConfig(raw, &w); err != nil {
				return err
			}
			baseline := productioncontrol.Baseline{Scope: scope,
				Verified: productioncontrol.Version{Generation: w.Generation, Digest: w.VerifiedDigest}, EvidenceDigest: evidence}
			created, err := j.ImportSampleRetentionBaseline(ctx, baseline, p, db, settings.LegacyRevocationEvidence,
				source, settings.ObserverKeyID, settings.ObserverPublicKey)
			if err != nil {
				return err
			}
			return json.NewEncoder(out).Encode(productioncontrol.Recorded{Digest: evidence, Created: created})
		})
}
