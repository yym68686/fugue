package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
)

const registryRehomeRecoveryFilename = "registry-rehome-recovery.json"

func packRegistryRehomeRecovery(args []string, out io.Writer) error {
	if len(args) != 4 || !filepath.IsAbs(args[0]) || !filepath.IsAbs(args[2]) {
		return errors.New(usage)
	}
	archive, err := readFile(args[0], releaseguardian.MaxReleaseArchiveBytes, true)
	if err != nil {
		return err
	}
	defer clear(archive)
	review, err := readFile(args[2], productioncontrol.MaxSubmissionBytes, false)
	if err != nil {
		return err
	}
	submission, err := productioncontrol.DecodeSubmission(review)
	if err != nil {
		return err
	}
	raw, report, err := releaseguardian.BuildRegistryRehomeRecoveryBundle(archive, args[1], submission)
	if err != nil {
		return err
	}
	defer clear(raw)
	if err := writePrivateReleaseRecovery(args[3], registryRehomeRecoveryFilename, raw); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func registryRehomeReviewCommand(ctx context.Context, args []string, out io.Writer) error {
	draft := args[0] == "draft-release-registry-rehome"
	if (!draft && len(args) != 6) || (draft && len(args) != 9) {
		return errors.New(usage)
	}
	archivePath, pin, group, kubeconfig, kubeContext := args[1], args[2], args[3], args[4], args[5]
	if draft {
		archivePath, pin, group, kubeconfig, kubeContext = args[4], args[5], args[6], args[2], args[3]
	}
	if !filepath.IsAbs(archivePath) {
		return errors.New("registry rehome requires a private absolute archive path")
	}
	raw, err := readFile(archivePath, releaseguardian.MaxReleaseArchiveBytes, true)
	if err != nil {
		return err
	}
	defer clear(raw)
	client, err := sealedRecoveryProvider(kubeconfig, kubeContext)
	if err != nil {
		return err
	}
	emit := json.NewEncoder(out).Encode
	if !draft {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		source, err := releaseguardian.ObserveRegistryRehomeSource(ctx, client, raw, pin, group)
		if err != nil {
			return err
		}
		return emit(source)
	}
	var cfg struct {
		Registry           string `json:"registry"`
		RegistryAuthFile   string `json:"registryAuthFile,omitempty"`
		ForwardRepository  string `json:"forwardRepository"`
		RollbackRepository string `json:"rollbackRepository"`
	}
	configuration, err := readFile(args[8], 16<<10, false)
	if err != nil {
		return err
	}
	if decodeRecoveryMirrorJSON(configuration, &cfg) != nil || cfg.ForwardRepository == "" || cfg.ForwardRepository != cfg.RollbackRepository {
		return errors.New("registry rehome requires one independent repository for both images")
	}
	registry, err := newRecoveryRegistry(cfg.Registry, cfg.RegistryAuthFile)
	if err != nil {
		return err
	}
	return withJournalDeadline(ctx, args[1], true, 4*time.Minute, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
		submission, err := releaseguardian.DraftRegistryRehomeSubmission(ctx, client, j, registry, raw, pin, group, cluster, args[7], cfg.ForwardRepository)
		if err != nil {
			return err
		}
		return emit(submission)
	})
}
