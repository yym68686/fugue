package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"fugue/internal/releasedomain"
	"fugue/internal/releaseevidence"
)

const (
	maxPrivateTraceBytes   = 8 << 10
	maxPrivateTraceEvents  = 64
	genesisEvidencePolicy  = "exact-sha-no-write-genesis-v1"
	privateTraceAPIVersion = "release-transaction-trace.fugue.dev/v1"
	privateTraceKind       = "ReleaseTransactionTraceEvent"
)

type privateTracePhase string

const (
	privateTracePhaseTransaction privateTracePhase = "transaction"
	privateTracePhasePrepare     privateTracePhase = "prepare"
	privateTracePhaseApply       privateTracePhase = "apply"
	privateTracePhaseVerify      privateTracePhase = "verify"
	privateTracePhaseRollback    privateTracePhase = "rollback"
)

type privateTraceState string

const (
	privateTraceStateStarted   privateTraceState = "started"
	privateTraceStateSucceeded privateTraceState = "succeeded"
	privateTraceStateFailed    privateTraceState = "failed"
)

type privateTraceEvent struct {
	APIVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Sequence   uint64               `json:"sequence"`
	Domain     releasedomain.Domain `json:"domain"`
	PlanDigest string               `json:"planDigest"`
	Phase      privateTracePhase    `json:"phase"`
	State      privateTraceState    `json:"state"`
}

type explicitBoolFlag struct {
	set   bool
	value bool
}

func (value *explicitBoolFlag) String() string {
	if value == nil {
		return "false"
	}
	return strconv.FormatBool(value.value)
}

func (value *explicitBoolFlag) Set(raw string) error {
	if value.set {
		return fmt.Errorf("boolean flag was supplied more than once")
	}
	value.set = true
	switch raw {
	case "true":
		value.value = true
		return nil
	case "false":
		value.value = false
		return nil
	default:
		return fmt.Errorf("boolean flag must be exactly true or false")
	}
}

type publicEvidenceOptions struct {
	bundleDir            string
	traceFile            string
	output               string
	runID                string
	runAttempt           uint64
	headSHA              string
	writeBoundaryCrossed explicitBoolFlag
	rollbackAttempted    explicitBoolFlag
	rollbackCompleted    explicitBoolFlag
	rollbackFailed       explicitBoolFlag
}

type genesisEvidenceOptions struct {
	ownershipPath       string
	changedEvidencePath string
	expectedChanges     stringListFlags
	output              string
	runID               string
	runAttempt          uint64
	expectedHeadSHA     string
	headSHA             string
	expectedParentSHA   string
	actualParentSHA     string
}

type stringListFlags []string

func (values *stringListFlags) String() string {
	if values == nil {
		return ""
	}
	return strings.Join(*values, ",")
}

func (values *stringListFlags) Set(value string) error {
	if !validExpectedChange(value) {
		return fmt.Errorf("expected change is invalid")
	}
	for _, existing := range *values {
		if existing == value {
			return fmt.Errorf("expected change is duplicated")
		}
	}
	*values = append(*values, value)
	return nil
}

type blockedEvidenceOptions struct {
	ownershipPath       string
	changedEvidencePath string
	trustedBaseCommit   string
	trustedTargetCommit string
	helmRevision        string
	namespace           string
	bindings            bindingFlags
	output              string
	runID               string
	runAttempt          uint64
	headSHA             string
}

func runWritePublicEvidenceCommand(args []string, stdout, stderr io.Writer) int {
	_ = stdout
	options, ok := parsePublicEvidenceFlags(args)
	if !ok {
		return fixedPublicEvidenceFailure(stderr, "public evidence flags are invalid")
	}
	if !publicOutputIsDisjoint(options.output, []string{options.traceFile}, []string{options.bundleDir, filepath.Dir(options.traceFile)}) {
		return fixedPublicEvidenceFailure(stderr, "public evidence output boundary is invalid")
	}
	verified, err := inspectAuthorizationBundle(options.bundleDir)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "authorization bundle verification failed")
	}
	if options.headSHA != verified.Decision.TrustedTargetCommit {
		return fixedPublicEvidenceFailure(stderr, "public evidence revision binding failed")
	}
	traceData, err := readSecureTraceSource(options.traceFile, maxPrivateTraceBytes)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "private transaction trace verification failed")
	}
	trace, err := decodePrivateTrace(traceData, verified.Decision)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "private transaction trace verification failed")
	}
	artifact, err := buildPublicArtifact(verified, trace, options)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "public evidence construction failed")
	}
	if err := releaseevidence.WritePrivateAtomic(options.output, artifact); err != nil {
		return fixedPublicEvidenceFailure(stderr, "public evidence publication failed")
	}
	return 0
}

func runWriteGenesisPublicEvidenceCommand(args []string, stdout, stderr io.Writer) int {
	_ = stdout
	options, ok := parseGenesisEvidenceFlags(args)
	if !ok {
		return fixedPublicEvidenceFailure(stderr, "genesis evidence flags are invalid")
	}
	if options.expectedHeadSHA != options.headSHA || options.expectedParentSHA != options.actualParentSHA {
		return fixedPublicEvidenceFailure(stderr, "genesis revision binding failed")
	}
	if !publicOutputIsDisjoint(
		options.output,
		[]string{options.ownershipPath, options.changedEvidencePath},
		[]string{filepath.Dir(options.ownershipPath), filepath.Dir(options.changedEvidencePath)},
	) {
		return fixedPublicEvidenceFailure(stderr, "genesis output boundary is invalid")
	}
	ownership, err := readSecureSource(options.ownershipPath, maxOwnershipBytes, false)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "genesis ownership verification failed")
	}
	if _, err := releasedomain.LoadOwnership(bytes.NewReader(ownership)); err != nil {
		return fixedPublicEvidenceFailure(stderr, "genesis ownership verification failed")
	}
	evidenceData, err := readSecureSource(options.changedEvidencePath, maxChangedEvidenceBytes, true)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "genesis changed-file evidence verification failed")
	}
	evidence, err := releasedomain.DecodeAndVerifyChangedFileEvidence(
		bytes.NewReader(evidenceData),
		options.actualParentSHA,
		options.headSHA,
	)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "genesis changed-file evidence verification failed")
	}
	if !exactChangedPathSet(evidence.Changes(), []string(options.expectedChanges)) {
		return fixedPublicEvidenceFailure(stderr, "genesis changed-file set verification failed")
	}
	artifact := buildGenesisPublicArtifact(options, evidence.Digest(), digestBytes(ownership))
	if err := releaseevidence.Validate(artifact); err != nil {
		return fixedPublicEvidenceFailure(stderr, "genesis evidence construction failed")
	}
	if err := releaseevidence.WritePrivateAtomic(options.output, artifact); err != nil {
		return fixedPublicEvidenceFailure(stderr, "genesis evidence publication failed")
	}
	return 0
}

func runWriteBlockedPublicEvidenceCommand(args []string, stdout, stderr io.Writer) int {
	_ = stdout
	options, ok := parseBlockedEvidenceFlags(args)
	if !ok {
		return fixedPublicEvidenceFailure(stderr, "blocked evidence flags are invalid")
	}
	if options.headSHA != options.trustedTargetCommit {
		return fixedPublicEvidenceFailure(stderr, "blocked evidence revision binding failed")
	}
	if !publicOutputIsDisjoint(
		options.output,
		[]string{options.ownershipPath, options.changedEvidencePath},
		[]string{filepath.Dir(options.ownershipPath), filepath.Dir(options.changedEvidencePath)},
	) {
		return fixedPublicEvidenceFailure(stderr, "blocked evidence output boundary is invalid")
	}
	ownership, err := readSecureSource(options.ownershipPath, maxOwnershipBytes, false)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "blocked ownership verification failed")
	}
	changedEvidence, err := readSecureSource(options.changedEvidencePath, maxChangedEvidenceBytes, true)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "blocked changed-file evidence verification failed")
	}
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil || spec.ValidateBindings(map[string]string(options.bindings)) != nil {
		return fixedPublicEvidenceFailure(stderr, "blocked ownership binding verification failed")
	}
	plan, err := releasedomain.BuildPlanFromArtifacts(releasedomain.PlanArtifactInput{
		Ownership:                 ownership,
		ChangedFileEvidence:       changedEvidence,
		TrustedBaseCommit:         options.trustedBaseCommit,
		TrustedTargetCommit:       options.trustedTargetCommit,
		BaseCanonicalManifest:     []byte{},
		TargetCanonicalManifest:   []byte{},
		RepeatedCanonicalManifest: []byte{},
		SSoTBaseDigest:            options.helmRevision,
		SSoTTargetDigest:          options.helmRevision,
		SSoTLiveDigest:            options.helmRevision,
		DefaultNamespace:          options.namespace,
		Bindings:                  map[string]string(options.bindings),
		IgnoreHelmTestHooks:       false,
	})
	if err != nil || (plan.Result != releasedomain.OutcomeMultiple && plan.Result != releasedomain.OutcomeUnknown) {
		return fixedPublicEvidenceFailure(stderr, "blocked planner outcome is not publishable")
	}
	helmRevision, err := parseCanonicalPositiveUint(options.helmRevision)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "blocked Helm revision is invalid")
	}
	renderedDigest, err := digestRenderedEvidence(plan.Rendered)
	if err != nil {
		return fixedPublicEvidenceFailure(stderr, "blocked rendered evidence digest failed")
	}
	emptyRenderedDigest, err := digestRenderedEvidence(releasedomain.RenderedClassification{
		Domains:  []releasedomain.Domain{},
		Evidence: []releasedomain.Evidence{},
	})
	if err != nil || renderedDigest != emptyRenderedDigest {
		return fixedPublicEvidenceFailure(stderr, "blocked rendered evidence is not canonical empty evidence")
	}
	artifact := releaseevidence.PublicArtifact{
		APIVersion:            releaseevidence.PublicAPIVersion,
		Kind:                  releaseevidence.PublicKind,
		RunID:                 options.runID,
		RunAttempt:            options.runAttempt,
		HeadSHA:               options.headSHA,
		HelmBaseRevision:      helmRevision,
		HelmTargetRevision:    helmRevision,
		Outcome:               releaseevidence.Outcome(plan.Result),
		PlanDigest:            plan.PlanDigest,
		ChangedEvidenceDigest: plan.Digests.ChangedFiles,
		BaseDigest:            plan.Digests.BaseManifest,
		TargetDigest:          plan.Digests.TargetManifest,
		RepeatDigest:          plan.Digests.RepeatedTargetManifest,
		OwnershipDigest:       plan.Digests.Ownership,
		ContextDigest:         plan.Digests.ClassificationContext.Digest,
		ForwardDigest:         renderedDigest,
		ReverseDigest:         renderedDigest,
		Trace:                 []releaseevidence.TraceEvent{},
	}
	if err := releaseevidence.Validate(artifact); err != nil {
		return fixedPublicEvidenceFailure(stderr, "blocked public evidence construction failed")
	}
	if err := releaseevidence.WritePrivateAtomic(options.output, artifact); err != nil {
		return fixedPublicEvidenceFailure(stderr, "blocked public evidence publication failed")
	}
	return 0
}

func parsePublicEvidenceFlags(args []string) (publicEvidenceOptions, bool) {
	policy := map[string]bool{
		"bundle-dir": false, "trace-file": false, "output": false,
		"run-id": false, "run-attempt": false, "head-sha": false,
		"write-boundary-crossed": false, "rollback-attempted": false,
		"rollback-completed": false, "rollback-failed": false,
	}
	if rejectDuplicateFlags(args, policy) != nil {
		return publicEvidenceOptions{}, false
	}
	var options publicEvidenceOptions
	var runAttempt string
	flags := flag.NewFlagSet("write-public-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.bundleDir, "bundle-dir", "", "")
	flags.StringVar(&options.traceFile, "trace-file", "", "")
	flags.StringVar(&options.output, "output", "", "")
	flags.StringVar(&options.runID, "run-id", "", "")
	flags.StringVar(&runAttempt, "run-attempt", "", "")
	flags.StringVar(&options.headSHA, "head-sha", "", "")
	flags.Var(&options.writeBoundaryCrossed, "write-boundary-crossed", "")
	flags.Var(&options.rollbackAttempted, "rollback-attempted", "")
	flags.Var(&options.rollbackCompleted, "rollback-completed", "")
	flags.Var(&options.rollbackFailed, "rollback-failed", "")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return publicEvidenceOptions{}, false
	}
	for _, value := range []string{options.bundleDir, options.traceFile, options.output, options.runID, runAttempt, options.headSHA} {
		if !utf8.ValidString(value) || value == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
			return publicEvidenceOptions{}, false
		}
	}
	if !options.writeBoundaryCrossed.set || !options.rollbackAttempted.set || !options.rollbackCompleted.set || !options.rollbackFailed.set {
		return publicEvidenceOptions{}, false
	}
	parsedAttempt, err := parseCanonicalPositiveUint(runAttempt)
	if err != nil || validateGitCommit(options.headSHA) != nil || !isCanonicalPositiveDecimal(options.runID) {
		return publicEvidenceOptions{}, false
	}
	options.runAttempt = parsedAttempt
	return options, true
}

func parseGenesisEvidenceFlags(args []string) (genesisEvidenceOptions, bool) {
	policy := map[string]bool{
		"ownership": false, "changed-evidence": false, "expected-change": true,
		"output": false, "run-id": false, "run-attempt": false,
		"expected-head-sha": false, "head-sha": false,
		"expected-parent-sha": false, "actual-parent-sha": false,
	}
	if rejectDuplicateFlags(args, policy) != nil {
		return genesisEvidenceOptions{}, false
	}
	var options genesisEvidenceOptions
	var runAttempt string
	flags := flag.NewFlagSet("write-genesis-public-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.ownershipPath, "ownership", "", "")
	flags.StringVar(&options.changedEvidencePath, "changed-evidence", "", "")
	flags.Var(&options.expectedChanges, "expected-change", "")
	flags.StringVar(&options.output, "output", "", "")
	flags.StringVar(&options.runID, "run-id", "", "")
	flags.StringVar(&runAttempt, "run-attempt", "", "")
	flags.StringVar(&options.expectedHeadSHA, "expected-head-sha", "", "")
	flags.StringVar(&options.headSHA, "head-sha", "", "")
	flags.StringVar(&options.expectedParentSHA, "expected-parent-sha", "", "")
	flags.StringVar(&options.actualParentSHA, "actual-parent-sha", "", "")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return genesisEvidenceOptions{}, false
	}
	for _, value := range []string{
		options.ownershipPath, options.changedEvidencePath, options.output, options.runID, runAttempt,
		options.expectedHeadSHA, options.headSHA, options.expectedParentSHA, options.actualParentSHA,
	} {
		if !utf8.ValidString(value) || value == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
			return genesisEvidenceOptions{}, false
		}
	}
	if len(options.expectedChanges) == 0 {
		return genesisEvidenceOptions{}, false
	}
	for _, commit := range []string{options.expectedHeadSHA, options.headSHA, options.expectedParentSHA, options.actualParentSHA} {
		if validateGitCommit(commit) != nil {
			return genesisEvidenceOptions{}, false
		}
	}
	parsedAttempt, err := parseCanonicalPositiveUint(runAttempt)
	if err != nil || !isCanonicalPositiveDecimal(options.runID) {
		return genesisEvidenceOptions{}, false
	}
	options.runAttempt = parsedAttempt
	return options, true
}

func parseBlockedEvidenceFlags(args []string) (blockedEvidenceOptions, bool) {
	policy := map[string]bool{
		"ownership": false, "changed-evidence": false,
		"trusted-base-commit": false, "trusted-target-commit": false,
		"helm-revision": false, "namespace": false, "binding": true,
		"output": false, "run-id": false, "run-attempt": false, "head-sha": false,
	}
	if rejectDuplicateFlags(args, policy) != nil {
		return blockedEvidenceOptions{}, false
	}
	var options blockedEvidenceOptions
	var runAttempt string
	flags := flag.NewFlagSet("write-blocked-public-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.ownershipPath, "ownership", "", "")
	flags.StringVar(&options.changedEvidencePath, "changed-evidence", "", "")
	flags.StringVar(&options.trustedBaseCommit, "trusted-base-commit", "", "")
	flags.StringVar(&options.trustedTargetCommit, "trusted-target-commit", "", "")
	flags.StringVar(&options.helmRevision, "helm-revision", "", "")
	flags.StringVar(&options.namespace, "namespace", "", "")
	flags.Var(&options.bindings, "binding", "")
	flags.StringVar(&options.output, "output", "", "")
	flags.StringVar(&options.runID, "run-id", "", "")
	flags.StringVar(&runAttempt, "run-attempt", "", "")
	flags.StringVar(&options.headSHA, "head-sha", "", "")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return blockedEvidenceOptions{}, false
	}
	for _, value := range []string{
		options.ownershipPath, options.changedEvidencePath,
		options.trustedBaseCommit, options.trustedTargetCommit,
		options.helmRevision, options.namespace, options.output,
		options.runID, runAttempt, options.headSHA,
	} {
		if !utf8.ValidString(value) || value == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
			return blockedEvidenceOptions{}, false
		}
	}
	for _, commit := range []string{options.trustedBaseCommit, options.trustedTargetCommit, options.headSHA} {
		if validateGitCommit(commit) != nil {
			return blockedEvidenceOptions{}, false
		}
	}
	if options.bindings == nil {
		options.bindings = bindingFlags{}
	}
	if current, exists := options.bindings["releaseNamespace"]; exists && current != options.namespace {
		return blockedEvidenceOptions{}, false
	}
	options.bindings["releaseNamespace"] = options.namespace
	parsedAttempt, err := parseCanonicalPositiveUint(runAttempt)
	if err != nil || !isCanonicalPositiveDecimal(options.runID) {
		return blockedEvidenceOptions{}, false
	}
	if _, err := parseCanonicalPositiveUint(options.helmRevision); err != nil {
		return blockedEvidenceOptions{}, false
	}
	options.runAttempt = parsedAttempt
	return options, true
}

func decodePrivateTrace(data []byte, decision dispatchDecision) ([]releaseevidence.TraceEvent, error) {
	if len(data) == 0 {
		return []releaseevidence.TraceEvent{}, nil
	}
	if data[len(data)-1] != '\n' {
		return nil, fmt.Errorf("private trace is not canonical JSONL")
	}
	lines := bytes.Split(data[:len(data)-1], []byte{'\n'})
	if len(lines) > maxPrivateTraceEvents {
		return nil, fmt.Errorf("private trace event count is invalid")
	}
	result := make([]releaseevidence.TraceEvent, 0, len(lines))
	for index, line := range lines {
		if len(line) == 0 {
			return nil, fmt.Errorf("private trace contains an empty line")
		}
		var event privateTraceEvent
		if err := decodeStrictJSON(line, &event); err != nil {
			return nil, fmt.Errorf("private trace event schema is invalid")
		}
		canonical, err := json.Marshal(event)
		if err != nil || !bytes.Equal(canonical, line) {
			return nil, fmt.Errorf("private trace event is not canonical JSON")
		}
		if event.APIVersion != privateTraceAPIVersion ||
			event.Kind != privateTraceKind ||
			event.Sequence != uint64(index+1) ||
			string(event.Domain) != decision.SelectedDomain ||
			event.PlanDigest != decision.PlanDigest {
			return nil, fmt.Errorf("private trace event binding is invalid")
		}
		phase, state, ok := projectPrivateTraceEvent(event)
		if !ok {
			return nil, fmt.Errorf("private trace event enum is invalid")
		}
		result = append(result, releaseevidence.TraceEvent{Phase: phase, State: state})
	}
	return result, nil
}

func projectPrivateTraceEvent(event privateTraceEvent) (releaseevidence.TracePhase, releaseevidence.TraceState, bool) {
	var phase releaseevidence.TracePhase
	switch event.Phase {
	case privateTracePhaseTransaction:
		phase = releaseevidence.TracePhaseTransaction
	case privateTracePhasePrepare:
		phase = releaseevidence.TracePhasePrepare
	case privateTracePhaseApply:
		phase = releaseevidence.TracePhaseApply
	case privateTracePhaseVerify:
		phase = releaseevidence.TracePhaseVerify
	case privateTracePhaseRollback:
		phase = releaseevidence.TracePhaseRollback
	default:
		return "", "", false
	}
	var state releaseevidence.TraceState
	switch event.State {
	case privateTraceStateStarted:
		state = releaseevidence.TraceStateStarted
	case privateTraceStateSucceeded:
		state = releaseevidence.TraceStateSucceeded
	case privateTraceStateFailed:
		state = releaseevidence.TraceStateFailed
	default:
		return "", "", false
	}
	return phase, state, true
}

func buildPublicArtifact(
	verified verifiedAuthorizationBundle,
	trace []releaseevidence.TraceEvent,
	options publicEvidenceOptions,
) (releaseevidence.PublicArtifact, error) {
	plan := verified.Plan
	baseRevision, err := parseCanonicalPositiveUint(plan.Digests.Base)
	if err != nil {
		return releaseevidence.PublicArtifact{}, fmt.Errorf("base revision is invalid")
	}
	forwardDigest, err := digestRenderedEvidence(plan.Rendered)
	if err != nil {
		return releaseevidence.PublicArtifact{}, err
	}
	reverseDigest := policyBoundDigest("reverse-not-authorized", plan.PlanDigest, forwardDigest)
	targetRevision := baseRevision
	artifact := releaseevidence.PublicArtifact{
		APIVersion:            releaseevidence.PublicAPIVersion,
		Kind:                  releaseevidence.PublicKind,
		RunID:                 options.runID,
		RunAttempt:            options.runAttempt,
		HeadSHA:               options.headSHA,
		HelmBaseRevision:      baseRevision,
		HelmTargetRevision:    targetRevision,
		Outcome:               releaseevidence.Outcome(plan.Result),
		PlanDigest:            plan.PlanDigest,
		ChangedEvidenceDigest: plan.Digests.ChangedFiles,
		BaseDigest:            plan.Digests.BaseManifest,
		TargetDigest:          plan.Digests.TargetManifest,
		RepeatDigest:          plan.Digests.RepeatedTargetManifest,
		OwnershipDigest:       plan.Digests.Ownership,
		ContextDigest:         plan.Digests.ClassificationContext.Digest,
		ForwardDigest:         forwardDigest,
		ReverseDigest:         reverseDigest,
		Trace:                 trace,
		WriteBoundaryCrossed:  options.writeBoundaryCrossed.value,
		RollbackAttempted:     options.rollbackAttempted.value,
		RollbackCompleted:     options.rollbackCompleted.value,
		RollbackFailed:        options.rollbackFailed.value,
	}
	if plan.Result == releasedomain.OutcomeSingle {
		if verified.ExecutionBinding == nil || verified.RollbackEvidence == nil {
			return releaseevidence.PublicArtifact{}, fmt.Errorf("single execution proof is missing")
		}
		targetRevision, err = parseCanonicalPositiveUint(verified.ExecutionBinding.TargetRevision)
		if err != nil || verified.ExecutionBinding.BaseRevision != plan.Digests.Base {
			return releaseevidence.PublicArtifact{}, fmt.Errorf("single revision binding is invalid")
		}
		objectCount, err := exactRenderedObjectCount(plan.Rendered, plan.SelectedDomain)
		if err != nil {
			return releaseevidence.PublicArtifact{}, err
		}
		if forwardDigest != verified.RollbackEvidence.ForwardEvidenceDigest ||
			verified.RollbackEvidence.ForwardEvidenceDigest != verified.RollbackEvidence.ReverseEvidenceDigest {
			return releaseevidence.PublicArtifact{}, fmt.Errorf("single reverse digest binding is invalid")
		}
		artifact.HelmTargetRevision = targetRevision
		artifact.Domain = plan.SelectedDomain
		artifact.ForwardDigest = verified.RollbackEvidence.ForwardEvidenceDigest
		artifact.ReverseDigest = verified.RollbackEvidence.ReverseEvidenceDigest
		artifact.ReverseAuthorized = true
		artifact.ReverseDomain = verified.RollbackEvidence.Domain
		artifact.ReverseObjectCount = objectCount
	} else {
		if len(trace) != 0 || options.writeBoundaryCrossed.value || options.rollbackAttempted.value || options.rollbackCompleted.value || options.rollbackFailed.value {
			return releaseevidence.PublicArtifact{}, fmt.Errorf("non-single evidence contains write state")
		}
	}
	if err := releaseevidence.Validate(artifact); err != nil {
		return releaseevidence.PublicArtifact{}, fmt.Errorf("public evidence is inconsistent")
	}
	return artifact, nil
}

func buildGenesisPublicArtifact(options genesisEvidenceOptions, changedDigest, ownershipDigest string) releaseevidence.PublicArtifact {
	digest := func(label string) string {
		return policyBoundDigest(genesisEvidencePolicy, label, options.actualParentSHA, options.headSHA, changedDigest, ownershipDigest)
	}
	targetDigest := digest("target-repeat")
	return releaseevidence.PublicArtifact{
		APIVersion:            releaseevidence.PublicAPIVersion,
		Kind:                  releaseevidence.PublicKind,
		RunID:                 options.runID,
		RunAttempt:            options.runAttempt,
		HeadSHA:               options.headSHA,
		HelmBaseRevision:      0,
		HelmTargetRevision:    0,
		Outcome:               releaseevidence.OutcomeGenesisZero,
		PlanDigest:            digest("plan"),
		ChangedEvidenceDigest: changedDigest,
		BaseDigest:            digest("base"),
		TargetDigest:          targetDigest,
		RepeatDigest:          targetDigest,
		OwnershipDigest:       ownershipDigest,
		ContextDigest:         digest("classification-context-policy"),
		ForwardDigest:         digest("forward-no-write"),
		ReverseDigest:         digest("reverse-not-authorized"),
		Trace:                 []releaseevidence.TraceEvent{},
	}
}

func validExpectedChange(value string) bool {
	if !utf8.ValidString(value) || value == "" || strings.ContainsRune(value, '\x00') || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
		return false
	}
	if filepath.IsAbs(value) || strings.ContainsRune(value, '\\') || pathpkg.Clean(value) != value || value == "." || value == ".." || strings.HasPrefix(value, "../") {
		return false
	}
	return true
}

func exactChangedPathSet(changes []releasedomain.ChangedFile, expected []string) bool {
	if len(changes) != len(expected) || len(expected) == 0 {
		return false
	}
	actual := make([]string, len(changes))
	actualSeen := make(map[string]struct{}, len(changes))
	for index, change := range changes {
		if !validExpectedChange(change.Path) {
			return false
		}
		if _, duplicate := actualSeen[change.Path]; duplicate {
			return false
		}
		actualSeen[change.Path] = struct{}{}
		actual[index] = change.Path
	}
	expectedCopy := append([]string(nil), expected...)
	sort.Strings(actual)
	sort.Strings(expectedCopy)
	for index := range actual {
		if actual[index] != expectedCopy[index] {
			return false
		}
	}
	return true
}

func exactRenderedObjectCount(classification releasedomain.RenderedClassification, domain releasedomain.Domain) (uint64, error) {
	if len(classification.Unknown) != 0 || len(classification.Domains) != 1 || classification.Domains[0] != domain {
		return 0, fmt.Errorf("rendered ownership is not exact single-domain evidence")
	}
	var count uint64
	for _, item := range classification.Evidence {
		if item.Ignored {
			continue
		}
		if item.Source != "rendered-object" || len(item.Domains) != 1 || item.Domains[0] != domain {
			return 0, fmt.Errorf("rendered object ownership is not exact")
		}
		count++
	}
	if count == 0 {
		return 0, fmt.Errorf("rendered object ownership is empty")
	}
	return count, nil
}

func digestRenderedEvidence(classification releasedomain.RenderedClassification) (string, error) {
	encoded, err := json.Marshal(classification)
	if err != nil {
		return "", fmt.Errorf("encode rendered evidence")
	}
	return digestBytes(encoded), nil
}

func policyBoundDigest(values ...string) string {
	encoded, err := json.Marshal(values)
	if err != nil {
		panic("encode fixed digest policy")
	}
	return digestBytes(encoded)
}

func parseCanonicalPositiveUint(value string) (uint64, error) {
	if !isCanonicalPositiveDecimal(value) {
		return 0, fmt.Errorf("value is not a canonical positive decimal")
	}
	return strconv.ParseUint(value, 10, 64)
}

func isCanonicalPositiveDecimal(value string) bool {
	if value == "" || len(value) > 20 || value[0] == '0' {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func publicOutputIsDisjoint(output string, protectedFiles, protectedDirectories []string) bool {
	resolvedOutput, err := resolveFuturePath(output)
	if err != nil {
		return false
	}
	for _, protected := range protectedFiles {
		resolved, err := filepath.EvalSymlinks(protected)
		if err != nil || filepath.Clean(resolved) == resolvedOutput {
			return false
		}
		if outputInfo, outputErr := os.Lstat(resolvedOutput); outputErr == nil {
			if protectedInfo, protectedErr := os.Stat(resolved); protectedErr == nil && os.SameFile(outputInfo, protectedInfo) {
				return false
			}
		}
	}
	for _, protected := range protectedDirectories {
		resolved, err := filepath.EvalSymlinks(protected)
		if err != nil {
			return false
		}
		resolved = filepath.Clean(resolved)
		if resolvedOutput == resolved || strings.HasPrefix(resolvedOutput, resolved+string(filepath.Separator)) {
			return false
		}
	}
	return true
}

func resolveFuturePath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func fixedPublicEvidenceFailure(stderr io.Writer, message string) int {
	_, _ = io.WriteString(stderr, "fugue-release-domain-dispatch: "+message+"\n")
	return 1
}
