package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"github.com/jackc/pgx/v5/stdlib"
)

func verifyReleaseArchive(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 2 || len(args) > 3 || !allDigests(args[1]) {
		return errors.New(usage)
	}
	raw, err := readPrivateReleaseArchive(args[0])
	if err != nil {
		return err
	}
	defer clear(raw)
	report, err := releaseguardian.VerifyReleaseRecoveryArchive(raw, args[1])
	if err != nil {
		return err
	}
	result := struct {
		releaseguardian.ReleaseRecoveryArchiveReport
		Mirror *releaseguardian.RegistryRecoveryReport `json:"mirror,omitempty"`
	}{ReleaseRecoveryArchiveReport: report}
	if len(args) == 3 {
		cfg, err := readReleaseMirrorConfig(args[2])
		if err != nil {
			return err
		}
		registry, err := openRegistry(cfg.Registry, cfg.RegistryAuthFile)
		if err != nil {
			return err
		}
		input := func(version releaseguardian.SealedRecoveryVersion, destination string) releaseguardian.RegistryRecoveryArtifact {
			return releaseguardian.RegistryRecoveryArtifact{Source: version.Receipt,
				SourceReceiptDigest: version.Monitor.ArtifactDigest, DestinationRepository: destination}
		}
		checked, err := registry.VerifyRecoveryMirror(ctx, input(report.Pair.Forward, cfg.ForwardRepository), input(report.Pair.Rollback, cfg.RollbackRepository))
		if err != nil {
			return err
		}
		result.Mirror = &checked
	}
	return json.NewEncoder(out).Encode(result)
}

type releaseMirrorConfig struct {
	Registry           string `json:"registry"`
	RegistryAuthFile   string `json:"registryAuthFile,omitempty"`
	ForwardRepository  string `json:"forwardRepository"`
	RollbackRepository string `json:"rollbackRepository"`
}

func readReleaseMirrorConfig(path string) (releaseMirrorConfig, error) {
	raw, err := readFile(path, 16<<10)
	if err != nil {
		return releaseMirrorConfig{}, err
	}
	var cfg releaseMirrorConfig
	if decode(raw, &cfg) != nil || cfg.ForwardRepository == "" || cfg.RollbackRepository == "" {
		return releaseMirrorConfig{}, errors.New("invalid archived release mirror configuration")
	}
	return cfg, nil
}

func draftReleaseRegistryRehome(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 3 || !allDigests(args[1]) {
		return errors.New(usage)
	}
	raw, err := readPrivateReleaseArchive(args[0])
	if err != nil {
		return err
	}
	defer clear(raw)
	cfg, err := readReleaseMirrorConfig(args[2])
	if err != nil {
		return err
	}
	registry, err := openRegistry(cfg.Registry, cfg.RegistryAuthFile)
	if err != nil {
		return err
	}
	draft, err := registry.DraftRegistryRehome(ctx, raw, args[1], cfg.ForwardRepository, cfg.RollbackRepository)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(draft)
}

func readPrivateReleaseArchive(path string) ([]byte, error) {
	return readPrivateReleaseRecovery(path, releaseguardian.MaxReleaseArchiveBytes)
}

func verifyRegistryRehomeRecovery(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 2 || len(args) > 3 || !allDigests(args[1]) {
		return errors.New(usage)
	}
	raw, err := readPrivateReleaseRecovery(args[0], releaseguardian.MaxRegistryRehomeRecoveryBytes)
	if err != nil {
		return err
	}
	defer clear(raw)
	var registry *releaseguardian.IndependentRegistry
	if len(args) == 3 {
		cfg, err := readRegistryEndpointConfig(args[2])
		if err != nil {
			return err
		}
		registry, err = openRegistry(cfg.Registry, cfg.RegistryAuthFile)
		if err != nil {
			return err
		}
	}
	report, err := releaseguardian.VerifyRegistryRehomeRecoveryBundle(ctx, raw, args[1], registry)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func verifyRegistryRehomeBaseline(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 4 || len(args) > 5 || !allDigests(args[1], args[3]) {
		return errors.New(usage)
	}
	raw, err := readPrivateReleaseRecovery(args[0], releaseguardian.MaxRegistryRehomeBaselineBytes)
	if err != nil {
		return err
	}
	defer clear(raw)
	bundle, err := readPrivateReleaseRecovery(args[2], releaseguardian.MaxRegistryRehomeRecoveryBytes)
	if err != nil {
		return err
	}
	defer clear(bundle)
	var registry *releaseguardian.IndependentRegistry
	if len(args) == 5 {
		cfg, err := readRegistryEndpointConfig(args[4])
		if err != nil {
			return err
		}
		registry, err = openRegistry(cfg.Registry, cfg.RegistryAuthFile)
		if err != nil {
			return err
		}
	}
	baseline, err := releaseguardian.VerifyRegistryRehomeRecoveryBaseline(ctx, raw, args[1], bundle, args[3], registry)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		ProposedBaseline releaseguardian.RegistryRehomeBaseline `json:"proposedBaseline"`
		MirrorReadBack   bool                                   `json:"mirrorReadBack"`
	}{baseline, registry != nil})
}

func readVerifiedRegistryRehomeBaseline(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 4 || !allDigests(args[3]) || !filepath.IsAbs(args[0]) || !filepath.IsAbs(args[1]) {
		return errors.New(usage)
	}
	var config struct {
		ResourceID string            `json:"resourceId"`
		Observer   ed25519.PublicKey `json:"observerPublicKey"`
	}
	raw, err := readFile(args[1], 16<<10)
	if err != nil || decode(raw, &config) != nil || config.ResourceID == "" || len(config.Observer) != ed25519.PublicKeySize {
		return errors.New("invalid independent registry rehome reader enrollment")
	}
	profile, err := readPrivateReleaseRecovery(args[0], 32<<10)
	clear(profile)
	if err != nil {
		return err
	}
	bundle, err := readPrivateReleaseRecovery(args[2], releaseguardian.MaxRegistryRehomeRecoveryBytes)
	if err != nil {
		return err
	}
	defer clear(bundle)
	dsn, owners, cluster, err := productioncontrol.LoadJournalEndpoint(args[0])
	if err != nil {
		return err
	}
	dsn.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*dsn)
	defer db.Close()
	db.SetMaxOpenConns(1)
	j, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	verified, err := releaseguardian.LoadVerifiedRegistryRehomeBaseline(ctx, j,
		productioncontrol.Scope{ClusterID: cluster, ResourceID: config.ResourceID}, config.Observer, bundle, args[3])
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Baseline releaseguardian.RegistryRehomeBaseline `json:"verifiedBaseline"`
		Evidence string                                 `json:"evidenceDigest"`
	}{verified.Baseline, verified.EvidenceDigest})
}

type registryEndpointConfig struct {
	Registry         string `json:"registry"`
	RegistryAuthFile string `json:"registryAuthFile,omitempty"`
}

func readRegistryEndpointConfig(path string) (registryEndpointConfig, error) {
	var cfg registryEndpointConfig
	raw, err := readFile(path, 16<<10)
	if err != nil {
		return cfg, err
	}
	if decode(raw, &cfg) != nil || cfg.Registry == "" {
		return cfg, errors.New("invalid registry rehome recovery endpoint configuration")
	}
	return cfg, nil
}

func readPrivateReleaseRecovery(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("release recovery archive requires an absolute path")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("release recovery archive is unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("release recovery archive must be private")
	}
	return boundedRead(file, limit)
}
