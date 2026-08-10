package releaseguardian

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"fugue/internal/declarativerelease"
)

var executionFileNames = []string{
	"artifact-receipt.json",
	"execution-plan.json",
	"forward.json",
	"lkg.json",
	"release-plan.json",
}

// ExecutionBundle is the byte-exact, prewrite plan produced by CI and
// consumed by the in-cluster Guardian. It contains no credentials and no
// executable hook.
type ExecutionBundle struct {
	Plan     declarativerelease.Plan
	Artifact declarativerelease.ArtifactReceipt
	Prepared declarativerelease.ExecutionPlan
	Release  declarativerelease.PlanRelease
	Forward  []byte
	LKG      []byte
	Files    map[string][]byte
}

func DecodeExecutionBundle(files map[string][]byte, key Key) (ExecutionBundle, error) {
	if key.Validate() != nil || len(files) != len(executionFileNames) {
		return ExecutionBundle{}, errors.New("Guardian execution bundle inventory is invalid")
	}
	got := make([]string, 0, len(files))
	canonical := make(map[string][]byte, len(files))
	for name, value := range files {
		got = append(got, name)
		trimmed := bytes.TrimSpace(value)
		if len(trimmed) == 0 || len(trimmed) > 4<<20 || strings.ContainsRune(name, '\x00') {
			return ExecutionBundle{}, fmt.Errorf("Guardian execution file %q is invalid", name)
		}
		canonical[name] = append([]byte(nil), trimmed...)
	}
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(executionFileNames, "\n") {
		return ExecutionBundle{}, errors.New("Guardian execution bundle file set is invalid")
	}
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(canonical["release-plan.json"]))
	if err != nil {
		return ExecutionBundle{}, err
	}
	if len(plan.Releases) != 1 {
		return ExecutionBundle{}, errors.New("Guardian execution plan must contain exactly one component")
	}
	release := plan.Releases[0]
	if release.ComponentID != key.Component || release.Delivery == nil || release.Delivery.Writer != "guardian" || release.Delivery.Group != key.Group {
		return ExecutionBundle{}, errors.New("Guardian execution plan delivery binding is invalid")
	}
	artifact, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(canonical["artifact-receipt.json"]))
	if err != nil {
		return ExecutionBundle{}, err
	}
	prepared, err := declarativerelease.DecodeRecordedExecutionPlan(bytes.NewReader(canonical["execution-plan.json"]), plan, canonical["forward.json"], canonical["lkg.json"])
	if err != nil {
		return ExecutionBundle{}, err
	}
	if prepared.Component != release.ComponentID || prepared.ConfigSHA != plan.HeadSHA || artifact.Component != prepared.Component ||
		artifact.ConfigSHA != prepared.ConfigSHA || artifact.ReceiptDigest != prepared.ArtifactDigest || artifact.ImmutableRef != prepared.Forward.ImageRef {
		return ExecutionBundle{}, errors.New("Guardian execution bundle artifact binding is invalid")
	}
	return ExecutionBundle{
		Plan: plan, Artifact: artifact, Prepared: prepared, Release: release,
		Forward: canonical["forward.json"], LKG: canonical["lkg.json"], Files: canonical,
	}, nil
}

func (bundle ExecutionBundle) ReleaseRecord(key Key, lkgRecordDigest string) (ReleaseRecord, error) {
	if !digestPattern.MatchString(lkgRecordDigest) {
		return ReleaseRecord{}, errors.New("Guardian LKG record digest is invalid")
	}
	healthRaw, err := declarativerelease.CanonicalJSON(bundle.Release.Health)
	if err != nil {
		return ReleaseRecord{}, err
	}
	return NewReleaseRecord(
		key,
		bundle.Prepared.ConfigSHA,
		bundle.Artifact.TopDigest,
		digest(bundle.Forward),
		lkgRecordDigest,
		digest(healthRaw),
	)
}

func executionFilesFromStrings(data map[string]string) (map[string][]byte, error) {
	files := make(map[string][]byte, len(executionFileNames))
	for _, name := range executionFileNames {
		value, exists := data[name]
		if !exists {
			return nil, fmt.Errorf("Guardian record lacks %q", name)
		}
		files[name] = []byte(value)
	}
	return files, nil
}
