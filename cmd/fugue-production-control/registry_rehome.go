package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"fugue/internal/releaseguardian/participation"
	kubejson "sigs.k8s.io/json"
)

const registryRehomeRecoveryFilename = "registry-rehome-recovery.json"

func pendingRegistryRehomeCommand(ctx context.Context, args []string, out io.Writer, mode string) error {
	settle := mode == "settle-registry-rehome-no-write"
	serving := mode == "observe-release-registry-rehome-serving"
	cutoverReadback := serving || mode == "observe-release-registry-rehome-cutover"
	count := 7
	if settle || serving {
		count = 9
	} else if cutoverReadback {
		count = 8
	}
	if len(args) != count || !filepath.IsAbs(args[3]) || !filepath.IsAbs(args[6]) ||
		((settle || serving) && !filepath.IsAbs(args[8])) || (cutoverReadback && !filepath.IsAbs(args[7])) {
		return errors.New(usage)
	}
	var cfg struct {
		Cutover  releaseguardian.KubeDeploymentCutover            `json:"cutover"`
		Provider productioncontrol.ParticipationKubernetesBinding `json:"provider"`
		Observer ed25519.PublicKey                                `json:"observerPublicKey"`
	}
	config, err := readFile(args[6], 16<<10, false)
	if err != nil {
		return err
	}
	if decodeRecoveryMirrorJSON(config, &cfg) != nil || len(cfg.Observer) != ed25519.PublicKeySize || cfg.Cutover.Phase != releaseguardian.KubeCutoverFrozen {
		return errors.New("invalid frozen registry rehome observation enrollment")
	}
	limit, timeout := int64(releaseguardian.MaxReleaseArchiveBytes), 30*time.Second
	if cutoverReadback {
		limit, timeout = releaseguardian.MaxRegistryRehomeRecoveryBytes, 4*time.Minute
	}
	archive, err := readFile(args[3], limit, true)
	if err != nil {
		return err
	}
	defer clear(archive)
	var registry *releaseguardian.IndependentRegistry
	var route releaseguardian.RegistryRehomeRoute
	if serving {
		raw, err := readFile(args[8], 16<<10, false)
		if err != nil {
			return err
		}
		if decodeRecoveryMirrorJSON(raw, &route) != nil || route.Validate() != nil {
			return errors.New("invalid registry rehome serving route enrollment")
		}
	}
	if cutoverReadback {
		var endpoint struct {
			Registry         string `json:"registry"`
			RegistryAuthFile string `json:"registryAuthFile,omitempty"`
		}
		raw, err := readFile(args[7], 16<<10, false)
		if err != nil {
			return err
		}
		if err := decodeRecoveryMirrorJSON(raw, &endpoint); err != nil {
			return err
		}
		registry, err = newRecoveryRegistry(endpoint.Registry, endpoint.RegistryAuthFile)
		if err != nil {
			return err
		}
	} else {
		if _, err := releaseguardian.VerifyReleaseRecoveryArchive(archive, args[4]); err != nil {
			return err
		}
	}
	kube, err := readKubeObservationConfig(args[1], args[2])
	if err != nil {
		return err
	}
	kube, err = participation.BindKubernetesConfig(kube, cfg.Provider)
	if err != nil {
		return err
	}
	client, err := sealedRecoveryObserver(kube)
	if err != nil {
		return err
	}
	var evidence []byte
	if settle {
		if !filepath.IsAbs(args[0]) {
			return errors.New("registry rehome settlement requires a private absolute journal profile")
		}
		profile, err := readFile(args[0], 32<<10, true)
		clear(profile)
		if err != nil {
			return err
		}
		evidence, err = readFile(args[8], productioncontrol.MaxWitnessBytes, true)
		if err != nil {
			return err
		}
		defer clear(evidence)
	}
	return withJournalDeadline(ctx, args[0], !settle, timeout, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
		if settle {
			result, err := releaseguardian.SettleRegistryRehomeWithoutWrite(ctx, client, j, cluster, args[7], args[5],
				cfg.Cutover, cfg.Provider, cfg.Observer, archive, args[4], evidence)
			if err != nil {
				return err
			}
			return json.NewEncoder(out).Encode(result)
		}
		if serving {
			observation, err := releaseguardian.ObserveRegistryRehomeServing(ctx, client, j, cluster, args[5],
				cfg.Cutover, cfg.Provider, cfg.Observer, archive, args[4], registry, route)
			if err != nil {
				return err
			}
			return json.NewEncoder(out).Encode(observation)
		}
		if cutoverReadback {
			observed, err := releaseguardian.ObserveRegistryRehomeCutover(ctx, client, j, cluster, args[5],
				cfg.Cutover, cfg.Provider, cfg.Observer, archive, args[4], registry)
			if err != nil {
				return err
			}
			return json.NewEncoder(out).Encode(observed)
		}
		observed, err := releaseguardian.ObserveFrozenRegistryRehomePending(ctx, client, j, cluster, args[5], cfg.Cutover, cfg.Provider, cfg.Observer, archive, args[4])
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(observed)
	})
}

func signRegistryRehomeNoWrite(args []string, out io.Writer, now func() time.Time) error {
	if len(args) != 4 {
		return errors.New(usage)
	}
	raw, err := readFile(args[0], productioncontrol.MaxWitnessBytes, true)
	if err != nil {
		return err
	}
	if fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) != args[1] {
		return errors.New("registry rehome closure differs from the reviewed file digest")
	}
	var e productioncontrol.RegistryRehomeNoWriteEvidence
	if strict, err := kubejson.UnmarshalStrict(raw, &e); err != nil || len(strict) != 0 || len(e.Signature) != 0 || e.ExpiresAt != 0 {
		return errors.New("registry rehome closure draft must be a single unsigned observation")
	}
	ttl, err := time.ParseDuration(args[3])
	if err != nil || ttl < time.Second || ttl > productioncontrol.MaxWitnessAge || ttl%time.Second != 0 {
		return errors.New("invalid registry rehome closure lifetime in whole seconds")
	}
	at := now().Unix()
	if e.ObservedAt <= 0 || e.ObservedAt > at || e.ObservedAt <= at-int64(ttl/time.Second) {
		return errors.New("registry rehome closure observation falls outside the signing window")
	}
	e.ExpiresAt = e.ObservedAt + int64(ttl/time.Second)
	input, err := e.SigningInput()
	if err != nil {
		return err
	}
	key, err := readSigningKey(args[2])
	if err != nil {
		return err
	}
	defer clear(key)
	e.Signature = ed25519.Sign(key, input)
	canonical, _, err := productioncontrol.EncodeRegistryRehomeNoWriteEvidence(e)
	if err != nil {
		return err
	}
	n, err := out.Write(canonical)
	if err == nil && n != len(canonical) {
		return io.ErrShortWrite
	}
	return err
}

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

func stageRegistryRehomeReleaseInput(args []string, out io.Writer) error {
	if len(args) != 4 || !filepath.IsAbs(args[0]) {
		return errors.New(usage)
	}
	bundle, err := readFile(args[0], releaseguardian.MaxRegistryRehomeRecoveryBytes, true)
	if err != nil {
		return err
	}
	defer clear(bundle)
	manifest, report, err := releaseguardian.ExportRegistryRehomeReleaseInput(bundle, args[1], args[2])
	if err != nil {
		return err
	}
	defer clear(manifest)
	if err := writePrivateReleaseRecovery(args[3], "manifest.json", manifest); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func registryRehomeBaselineCommand(args []string, out io.Writer, stage bool) error {
	if stage {
		if len(args) != 3 || !filepath.IsAbs(args[0]) {
			return errors.New(usage)
		}
		bundle, err := readFile(args[0], releaseguardian.MaxRegistryRehomeRecoveryBytes, true)
		if err != nil {
			return err
		}
		defer clear(bundle)
		raw, report, err := releaseguardian.BuildRegistryRehomeBaseline(bundle, args[1])
		if err != nil {
			return err
		}
		if err := writePrivateReleaseRecovery(args[2], "baseline.json", raw); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(report)
	}
	if len(args) != 4 || !filepath.IsAbs(args[0]) || !filepath.IsAbs(args[2]) {
		return errors.New(usage)
	}
	raw, err := readFile(args[0], releaseguardian.MaxRegistryRehomeBaselineBytes, true)
	if err != nil {
		return err
	}
	bundle, err := readFile(args[2], releaseguardian.MaxRegistryRehomeRecoveryBytes, true)
	if err != nil {
		return err
	}
	defer clear(bundle)
	report, err := releaseguardian.VerifyRegistryRehomeBaseline(raw, args[1], bundle, args[3])
	if err != nil {
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
