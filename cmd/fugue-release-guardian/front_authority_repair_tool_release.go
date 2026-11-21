package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const frontRepairToolArtifactPrefix = "fugue-edge-front-cas-linux-"

// frontRepairToolBuildManifest has no Kubernetes or execution path. It verifies
// the exact two clean builds in a fresh artifact directory before CI publishes
// them. It does not authenticate a CI run or authorize a Front repair.
func frontRepairToolBuildManifest(dir, source string) (frontRepairToolManifest, error) {
	if !exactSourceSHA(source) || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return frontRepairToolManifest{}, errors.New("Front repair tool build requires an exact revision and absolute artifact directory")
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() {
		return frontRepairToolManifest{}, errors.New("Front repair tool artifact path is not a plain directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return frontRepairToolManifest{}, err
	}
	defer root.Close()
	actual, err := root.Stat(".")
	if err != nil || !os.SameFile(info, actual) {
		return frontRepairToolManifest{}, errors.New("Front repair tool artifact directory changed")
	}
	list, err := root.Open(".")
	if err != nil {
		return frontRepairToolManifest{}, err
	}
	entries, readErr := list.ReadDir(3)
	closeErr := list.Close()
	if (readErr != nil && !errors.Is(readErr, io.EOF)) || closeErr != nil || len(entries) != 2 {
		return frontRepairToolManifest{}, errors.New("Front repair tool artifact directory must contain only the two Linux executables")
	}
	archs := []string{"amd64", "arm64"}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || (entry.Name() != frontRepairToolArtifactPrefix+archs[0] && entry.Name() != frontRepairToolArtifactPrefix+archs[1]) {
			return frontRepairToolManifest{}, errors.New("Front repair tool artifact contains a foreign or linked entry")
		}
	}
	manifest := frontRepairToolManifest{Schema: frontRepairToolManifestSchema, SourceCommit: source}
	binaries := make(map[string][]byte, len(archs))
	for _, arch := range archs {
		name := frontRepairToolArtifactPrefix + arch
		data, err := readFrontRepairToolArtifact(root, name)
		if err != nil {
			return frontRepairToolManifest{}, fmt.Errorf("Front repair Linux/%s artifact: %w", arch, err)
		}
		binaries[arch] = data
		manifest.Tools = append(manifest.Tools, frontRepairToolRecord{SourceCommit: source,
			Architecture: arch, BinaryDigest: shaDigest(data), BinaryBytes: len(data)})
	}
	manifest.Digest, err = manifest.validatedDigest()
	if err != nil {
		return frontRepairToolManifest{}, err
	}
	if _, err := loadFrontRepairToolSet(manifest, binaries); err != nil {
		return frontRepairToolManifest{}, err
	}
	return manifest, nil
}

func readFrontRepairToolArtifact(root *os.Root, name string) ([]byte, error) {
	info, err := root.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > frontRepairToolMaxBytes {
		return nil, errors.New("expected a regular file within the retained Worker tool budget")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	got, err := file.Stat()
	if err != nil || !os.SameFile(info, got) || got.Size() != info.Size() {
		return nil, errors.New("tool changed before being read")
	}
	data, err := io.ReadAll(io.LimitReader(file, frontRepairToolMaxBytes+1))
	if err != nil || int64(len(data)) != info.Size() {
		return nil, errors.New("tool byte count changed during read")
	}
	return data, nil
}

func runFrontRepairToolManifest(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("front-repair-tool-manifest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dir := flags.String("dir", "", "absolute directory containing only the amd64 and arm64 Linux tools")
	source := flags.String("source-commit", "", "exact clean build commit")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || flags.NFlag() != 2 || stdout == nil {
		return errors.New("front-repair-tool-manifest requires --dir and --source-commit")
	}
	manifest, err := frontRepairToolBuildManifest(*dir, *source)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(manifest)
}
