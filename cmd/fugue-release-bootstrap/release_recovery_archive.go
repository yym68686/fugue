package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"fugue/internal/releaseguardian"
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
		raw, err := readFile(args[2], 16<<10)
		if err != nil {
			return err
		}
		var cfg struct {
			Registry           string `json:"registry"`
			RegistryAuthFile   string `json:"registryAuthFile,omitempty"`
			ForwardRepository  string `json:"forwardRepository"`
			RollbackRepository string `json:"rollbackRepository"`
		}
		if decode(raw, &cfg) != nil || cfg.ForwardRepository == "" || cfg.RollbackRepository == "" {
			return errors.New("invalid archived release mirror configuration")
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

func readPrivateReleaseArchive(path string) ([]byte, error) {
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
	return boundedRead(file, releaseguardian.MaxReleaseArchiveBytes)
}
