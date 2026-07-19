package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	invalidInvocationMessage = "fugue-policy-baseline-ready-materialize: flags are invalid\n"
	invalidEvidenceMessage   = "fugue-policy-baseline-ready-materialize: evidence is invalid\n"
	publicationMessage       = "fugue-policy-baseline-ready-materialize: publication failed\n"
)

var (
	hexOIDPattern  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	timingStatuses = map[string]bool{"PASS": true, "TIMING_NONCOMPLIANT": true}
)

type options struct {
	checkpointPath    string
	environmentPath   string
	timingPath        string
	checkpointDigest  string
	environmentDigest string
	timingDigest      string
	expectedSHA       string
	expectedTree      string
}

type attestedInput struct {
	data   []byte
	digest string
	info   os.FileInfo
}

type checkpointDocument struct {
	Checkpoint string `json:"checkpoint"`
	Status     string `json:"status"`
	Commit     struct {
		SHA  string `json:"sha"`
		Tree string `json:"tree"`
	} `json:"commit"`
	FullGate struct {
		Result        string `json:"result"`
		CandidateTree string `json:"candidate_tree"`
	} `json:"full_gate"`
	IndependentReview struct {
		Result string `json:"result"`
		Ended  bool   `json:"ended"`
	} `json:"independent_review"`
	PlannerGate struct {
		Result             string             `json:"result"`
		Domains            *[]json.RawMessage `json:"domains"`
		ExitCode           int                `json:"exit_code"`
		ZeroRuntimeDomains bool               `json:"zero_runtime_domains"`
	} `json:"planner_gate"`
	Publication struct {
		MainSHA         string `json:"main_sha"`
		CIResult        string `json:"ci_result"`
		BuildResult     string `json:"build_result"`
		FormalResult    string `json:"formal_result"`
		ProductionWrite *bool  `json:"production_write"`
	} `json:"publication"`
	Observation struct {
		Result                    string `json:"result"`
		Samples                   int    `json:"samples"`
		MainStable                bool   `json:"main_stable"`
		ActionsAndArtifactsStable bool   `json:"actions_and_artifacts_stable"`
		APIHealth                 string `json:"api_health"`
		CentralCoreDNS            string `json:"central_coredns"`
	} `json:"observation"`
	RecoveryProof struct {
		Result             string `json:"result"`
		RemoteRefMutation  *bool  `json:"remote_ref_mutation"`
		ProductionMutation *bool  `json:"production_mutation"`
	} `json:"recovery_proof"`
}

type environmentDocument struct {
	SchemaVersion int       `json:"schema_version"`
	Status        string    `json:"status"`
	QualifiedAt   string    `json:"qualified_at_utc"`
	Class         string    `json:"environment_class"`
	DiskAvailable int64     `json:"disk_available_kib"`
	DiskMinimum   int64     `json:"disk_minimum_kib"`
	LoopbackPort  int       `json:"loopback_shared_udp_tcp_port"`
	Residual      *[]string `json:"residual_processes"`
	Toolchain     struct {
		Go   string `json:"go"`
		Git  string `json:"git"`
		Helm string `json:"helm"`
		GH   string `json:"gh"`
	} `json:"toolchain"`
	CanonicalUserFiles struct {
		Result string `json:"result"`
	} `json:"canonical_user_files"`
}

type timingDocument struct {
	Checkpoint    string `json:"checkpoint"`
	CandidateSHA  string `json:"candidate_sha"`
	CandidateTree string `json:"candidate_tree"`
	SafetyStatus  string `json:"safety_status"`
	TimingStatus  string `json:"timing_status"`
}

type baselineReadyDocument struct {
	SchemaVersion    int               `json:"schema_version"`
	Status           string            `json:"status"`
	BaseSHA          string            `json:"base_sha"`
	BaseTree         string            `json:"base_tree"`
	QualifiedAt      string            `json:"qualified_at_utc"`
	EnvironmentClass string            `json:"environment_class"`
	DiskAvailable    int64             `json:"disk_available_kib"`
	DiskMinimum      int64             `json:"disk_minimum_kib"`
	LoopbackPort     int               `json:"loopback_shared_udp_tcp_port"`
	Toolchain        map[string]string `json:"toolchain"`
	Source           struct {
		Checkpoint        string `json:"checkpoint"`
		CheckpointDigest  string `json:"checkpoint_digest"`
		EnvironmentDigest string `json:"environment_digest"`
		TimingDigest      string `json:"timing_digest"`
		SafetyStatus      string `json:"safety_status"`
		TimingStatus      string `json:"timing_status"`
	} `json:"source"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	config, err := parseOptions(args)
	if err != nil {
		_, _ = io.WriteString(stderr, invalidInvocationMessage)
		return 1
	}
	checkpointInput, err := readAttestedJSON(config.checkpointPath, config.checkpointDigest)
	if err != nil {
		_, _ = io.WriteString(stderr, invalidEvidenceMessage)
		return 1
	}
	environmentInput, err := readAttestedJSON(config.environmentPath, config.environmentDigest)
	if err != nil {
		_, _ = io.WriteString(stderr, invalidEvidenceMessage)
		return 1
	}
	timingInput, err := readAttestedJSON(config.timingPath, config.timingDigest)
	if err != nil {
		_, _ = io.WriteString(stderr, invalidEvidenceMessage)
		return 1
	}
	if os.SameFile(checkpointInput.info, environmentInput.info) || os.SameFile(checkpointInput.info, timingInput.info) ||
		os.SameFile(environmentInput.info, timingInput.info) {
		_, _ = io.WriteString(stderr, invalidEvidenceMessage)
		return 1
	}

	var checkpoint checkpointDocument
	var environment environmentDocument
	var timing timingDocument
	if json.Unmarshal(checkpointInput.data, &checkpoint) != nil || json.Unmarshal(environmentInput.data, &environment) != nil || json.Unmarshal(timingInput.data, &timing) != nil ||
		validateCheckpoint(checkpoint, config.expectedSHA, config.expectedTree) != nil ||
		validateEnvironment(environment) != nil || validateTiming(timing, checkpoint, config.expectedSHA, config.expectedTree) != nil {
		_, _ = io.WriteString(stderr, invalidEvidenceMessage)
		return 1
	}

	document := baselineReadyDocument{
		SchemaVersion:    1,
		Status:           "BASELINE_READY",
		BaseSHA:          config.expectedSHA,
		BaseTree:         config.expectedTree,
		QualifiedAt:      environment.QualifiedAt,
		EnvironmentClass: environment.Class,
		DiskAvailable:    environment.DiskAvailable,
		DiskMinimum:      environment.DiskMinimum,
		LoopbackPort:     environment.LoopbackPort,
		Toolchain: map[string]string{
			"gh": environment.Toolchain.GH, "git": environment.Toolchain.Git,
			"go": environment.Toolchain.Go, "helm": environment.Toolchain.Helm,
		},
	}
	document.Source.Checkpoint = checkpoint.Checkpoint
	document.Source.CheckpointDigest = checkpointInput.digest
	document.Source.EnvironmentDigest = environmentInput.digest
	document.Source.TimingDigest = timingInput.digest
	document.Source.SafetyStatus = timing.SafetyStatus
	document.Source.TimingStatus = timing.TimingStatus
	encoded, err := json.Marshal(document)
	if err != nil {
		_, _ = io.WriteString(stderr, publicationMessage)
		return 1
	}
	encoded = append(encoded, '\n')
	if _, err := stdout.Write(encoded); err != nil {
		_, _ = io.WriteString(stderr, publicationMessage)
		return 1
	}
	return 0
}

func parseOptions(args []string) (options, error) {
	var config options
	flags := flag.NewFlagSet("fugue-policy-baseline-ready-materialize", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&config.checkpointPath, "checkpoint", "", "completed checkpoint JSON")
	flags.StringVar(&config.environmentPath, "environment", "", "environment qualification JSON")
	flags.StringVar(&config.timingPath, "timing", "", "checkpoint timing JSON")
	flags.StringVar(&config.checkpointDigest, "checkpoint-digest", "", "expected checkpoint SHA-256")
	flags.StringVar(&config.environmentDigest, "environment-digest", "", "expected environment SHA-256")
	flags.StringVar(&config.timingDigest, "timing-digest", "", "expected timing SHA-256")
	flags.StringVar(&config.expectedSHA, "expected-sha", "", "exact main commit")
	flags.StringVar(&config.expectedTree, "expected-tree", "", "exact main tree")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return options{}, fmt.Errorf("invalid flags")
	}
	for _, value := range []string{config.checkpointPath, config.environmentPath, config.timingPath} {
		if strings.TrimSpace(value) == "" {
			return options{}, fmt.Errorf("missing path")
		}
	}
	for _, value := range []string{config.checkpointDigest, config.environmentDigest, config.timingDigest} {
		if !digestPattern.MatchString(value) {
			return options{}, fmt.Errorf("invalid digest")
		}
	}
	if !hexOIDPattern.MatchString(config.expectedSHA) || !hexOIDPattern.MatchString(config.expectedTree) {
		return options{}, fmt.Errorf("invalid identity")
	}
	return config, nil
}

func readAttestedJSON(path, expected string) (attestedInput, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil || !pathInfo.Mode().IsRegular() {
		return attestedInput{}, fmt.Errorf("evidence path is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return attestedInput{}, fmt.Errorf("open evidence")
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return attestedInput{}, fmt.Errorf("evidence identity changed")
	}
	data, err := io.ReadAll(file)
	if err != nil || len(data) == 0 || int64(len(data)) != openedInfo.Size() || !json.Valid(data) {
		return attestedInput{}, fmt.Errorf("read evidence")
	}
	afterInfo, err := file.Stat()
	if err != nil || !os.SameFile(openedInfo, afterInfo) || afterInfo.Size() != openedInfo.Size() {
		return attestedInput{}, fmt.Errorf("evidence changed while reading")
	}
	digest := sha256.Sum256(data)
	actual := "sha256:" + hex.EncodeToString(digest[:])
	if actual != expected {
		return attestedInput{}, fmt.Errorf("digest mismatch")
	}
	return attestedInput{data: data, digest: actual, info: openedInfo}, nil
}

func validateCheckpoint(value checkpointDocument, expectedSHA, expectedTree string) error {
	validBuild := value.Publication.BuildResult == "success" || value.Publication.BuildResult == "not_required_zero_runtime"
	if strings.TrimSpace(value.Checkpoint) == "" || value.Status != "checkpoint_complete" ||
		value.Commit.SHA != expectedSHA || value.Commit.Tree != expectedTree ||
		value.FullGate.Result != "PASS" || value.FullGate.CandidateTree != expectedTree ||
		value.IndependentReview.Result != "APPROVE" || !value.IndependentReview.Ended ||
		value.PlannerGate.Result != "unknown" || value.PlannerGate.Domains == nil || len(*value.PlannerGate.Domains) != 0 || value.PlannerGate.ExitCode != 2 || !value.PlannerGate.ZeroRuntimeDomains ||
		value.Publication.MainSHA != expectedSHA || value.Publication.CIResult != "success" || !validBuild || value.Publication.FormalResult != "success" || value.Publication.ProductionWrite == nil || *value.Publication.ProductionWrite ||
		value.Observation.Result != "PASS" || value.Observation.Samples < 5 || !value.Observation.MainStable || !value.Observation.ActionsAndArtifactsStable || value.Observation.APIHealth != "ok" || value.Observation.CentralCoreDNS != "5/5 1/1 1" ||
		value.RecoveryProof.Result != "PASS" || value.RecoveryProof.RemoteRefMutation == nil || *value.RecoveryProof.RemoteRefMutation ||
		value.RecoveryProof.ProductionMutation == nil || *value.RecoveryProof.ProductionMutation {
		return fmt.Errorf("checkpoint gate failed")
	}
	return nil
}

func validateEnvironment(value environmentDocument) error {
	qualifiedAt, err := time.Parse(time.RFC3339, value.QualifiedAt)
	if err != nil || qualifiedAt.IsZero() || value.SchemaVersion != 1 || value.Status != "ENVIRONMENT_READY" ||
		strings.TrimSpace(value.Class) == "" || value.DiskMinimum < 10485760 || value.DiskAvailable < value.DiskMinimum ||
		value.LoopbackPort < 1 || value.LoopbackPort > 65535 || value.Residual == nil || len(*value.Residual) != 0 || value.CanonicalUserFiles.Result != "PASS" ||
		strings.TrimSpace(value.Toolchain.Go) == "" || strings.TrimSpace(value.Toolchain.Git) == "" ||
		strings.TrimSpace(value.Toolchain.Helm) == "" || strings.TrimSpace(value.Toolchain.GH) == "" {
		return fmt.Errorf("environment gate failed")
	}
	return nil
}

func validateTiming(value timingDocument, checkpoint checkpointDocument, expectedSHA, expectedTree string) error {
	if value.Checkpoint != checkpoint.Checkpoint || value.CandidateSHA != expectedSHA || value.CandidateTree != expectedTree ||
		value.SafetyStatus != "PASS" || !timingStatuses[value.TimingStatus] {
		return fmt.Errorf("timing evidence failed")
	}
	return nil
}

func canonicalOneLF(data []byte) bool {
	return bytes.HasSuffix(data, []byte{'\n'}) && !bytes.HasSuffix(data, []byte{'\n', '\n'})
}
