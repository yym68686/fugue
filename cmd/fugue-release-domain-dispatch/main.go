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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"fugue/internal/releasedomain"
)

const (
	decisionAPIVersion = "release-domain-dispatch.fugue.dev/v1"
	decisionKind       = "ReleaseDomainDispatchDecision"

	planFilename             = "release-domain-plan.json"
	decisionFilename         = "decision.json"
	ownershipFilename        = "ownership.yaml"
	changedEvidenceFilename  = "changed-file-evidence.json"
	baseManifestFilename     = "base-manifest.yaml"
	targetManifestFilename   = "target-manifest.yaml"
	repeatedManifestFilename = "repeated-target-manifest.yaml"
	argvSnapshotFilename     = "upgrade-argv.snapshot"
	envelopeFilename         = "transaction-envelope.json"
	executionBindingFilename = "execution-binding.json"
	rollbackEvidenceFilename = "rollback-ownership-evidence.json"

	maxOwnershipBytes       = 4 << 20
	maxChangedEvidenceBytes = 32 << 20
	maxManifestBytes        = 8 << 20
	maxArgvSnapshotBytes    = 8 << 20
	maxPlanBytes            = 8 << 20
	maxEnvelopeBytes        = 8 << 20
	maxDecisionBytes        = 64 << 10
	maxBindingBytes         = 64 << 10
	maxRollbackEvidence     = 64 << 10
)

type bindingFlags map[string]string

func (values *bindingFlags) String() string {
	if values == nil || len(*values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*values))
	for key := range *values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+(*values)[key])
	}
	return strings.Join(parts, ",")
}

func (values *bindingFlags) Set(value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("binding must be valid UTF-8")
	}
	name, resolved, ok := strings.Cut(value, "=")
	if !ok || name == "" || resolved == "" || strings.TrimSpace(name) != name || strings.TrimSpace(resolved) != resolved {
		return fmt.Errorf("binding must be non-empty key=value without surrounding whitespace")
	}
	if strings.ContainsRune(name, '\x00') || strings.ContainsRune(resolved, '\x00') {
		return fmt.Errorf("binding contains NUL")
	}
	if *values == nil {
		*values = bindingFlags{}
	}
	if _, duplicate := (*values)[name]; duplicate {
		return fmt.Errorf("binding %q was supplied more than once", name)
	}
	(*values)[name] = resolved
	return nil
}

type explicitFalseFlag struct {
	set   bool
	value bool
}

func (value *explicitFalseFlag) String() string {
	if value == nil {
		return "false"
	}
	return fmt.Sprintf("%t", value.value)
}

func (value *explicitFalseFlag) Set(raw string) error {
	if value.set {
		return fmt.Errorf("flag was supplied more than once")
	}
	value.set = true
	switch raw {
	case "false":
		value.value = false
		return nil
	case "true":
		return fmt.Errorf("must be false; execution authorization requires canonical no-hooks manifests")
	default:
		return fmt.Errorf("must be exactly false")
	}
}

func (value *explicitFalseFlag) IsBoolFlag() bool { return true }

type authorizeOptions struct {
	ownershipPath        string
	changedEvidencePath  string
	trustedBaseCommit    string
	trustedTargetCommit  string
	baseManifestPath     string
	targetManifestPath   string
	repeatedManifestPath string
	argvSnapshotPath     string
	bundleDir            string
	releaseName          string
	releaseNamespace     string
	baseRevision         string
	targetRevision       string
	bindings             bindingFlags
	ignoreHooks          explicitFalseFlag
}

type artifactDigests struct {
	Plan                      string `json:"plan"`
	Ownership                 string `json:"ownership"`
	ChangedFileEvidence       string `json:"changedFileEvidence"`
	BaseManifest              string `json:"baseManifest"`
	TargetManifest            string `json:"targetManifest"`
	RepeatedTargetManifest    string `json:"repeatedTargetManifest"`
	UpgradeArgvSnapshot       string `json:"upgradeArgvSnapshot"`
	TransactionEnvelope       string `json:"transactionEnvelope"`
	ExecutionBinding          string `json:"executionBinding"`
	RollbackOwnershipEvidence string `json:"rollbackOwnershipEvidence"`
}

type dispatchDecision struct {
	APIVersion          string          `json:"apiVersion"`
	Kind                string          `json:"kind"`
	Outcome             string          `json:"outcome"`
	SelectedDomain      string          `json:"selectedDomain"`
	PlanDigest          string          `json:"planDigest"`
	TrustedBaseCommit   string          `json:"trustedBaseCommit"`
	TrustedTargetCommit string          `json:"trustedTargetCommit"`
	Artifacts           artifactDigests `json:"artifacts"`
}

type authorizationArtifacts struct {
	decision         dispatchDecision
	plan             releasedomain.Plan
	planJSON         []byte
	ownership        []byte
	changedEvidence  []byte
	baseManifest     []byte
	targetManifest   []byte
	repeatedManifest []byte
	argvSnapshot     []byte
	envelopeJSON     []byte
	bindingJSON      []byte
	rollbackJSON     []byte
}

type verifiedAuthorizationBundle struct {
	Decision         dispatchDecision
	Plan             releasedomain.Plan
	ExecutionBinding *releasedomain.ExecutionBinding
	RollbackEvidence *releasedomain.RollbackOwnershipEvidence
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "fugue-release-domain-dispatch: expected a fixed subcommand")
		return 1
	}
	switch args[0] {
	case "classify-files":
		return runClassifyFilesCommand(args[1:], stdout, stderr)
	case "write-public-evidence":
		return runWritePublicEvidenceCommand(args[1:], stdout, stderr)
	case "write-genesis-public-evidence":
		return runWriteGenesisPublicEvidenceCommand(args[1:], stdout, stderr)
	case "write-blocked-public-evidence":
		return runWriteBlockedPublicEvidenceCommand(args[1:], stdout, stderr)
	}
	var (
		outcome releasedomain.Outcome
		domain  releasedomain.Domain
		digest  string
		err     error
	)
	switch args[0] {
	case "authorize":
		outcome, domain, digest, err = runAuthorize(args[1:], stderr)
	case "verify":
		outcome, domain, digest, err = runVerify(args[1:], stderr)
	default:
		_, _ = io.WriteString(stderr, "fugue-release-domain-dispatch: fixed subcommand is invalid\n")
		return 1
	}
	if err != nil {
		_, _ = io.WriteString(stderr, "fugue-release-domain-dispatch: "+args[0]+" failed\n")
		return 1
	}
	switch outcome {
	case releasedomain.OutcomeZero:
		if _, err := fmt.Fprintf(stdout, "zero\t%s\n", digest); err != nil {
			_, _ = io.WriteString(stderr, "fugue-release-domain-dispatch: write fixed result failed\n")
			return 1
		}
		return 0
	case releasedomain.OutcomeSingle:
		if _, err := fmt.Fprintf(stdout, "single\t%s\t%s\n", domain, digest); err != nil {
			_, _ = io.WriteString(stderr, "fugue-release-domain-dispatch: write fixed result failed\n")
			return 1
		}
		return 0
	case releasedomain.OutcomeMultiple, releasedomain.OutcomeUnknown:
		return 2
	default:
		fmt.Fprintln(stderr, "fugue-release-domain-dispatch: invalid verified outcome")
		return 1
	}
}

func runAuthorize(args []string, stderr io.Writer) (releasedomain.Outcome, releasedomain.Domain, string, error) {
	_ = stderr
	options, err := parseAuthorizeFlags(args)
	if err != nil {
		return "", "", "", err
	}
	artifacts, err := buildAuthorization(options)
	if err != nil {
		return "", "", "", err
	}
	if err := publishAuthorizationBundle(options.bundleDir, artifacts); err != nil {
		return "", "", "", err
	}
	return artifacts.plan.Result, artifacts.plan.SelectedDomain, artifacts.plan.PlanDigest, nil
}

func runVerify(args []string, stderr io.Writer) (releasedomain.Outcome, releasedomain.Domain, string, error) {
	_ = stderr
	if err := rejectDuplicateFlags(args, map[string]bool{"bundle-dir": false}); err != nil {
		return "", "", "", err
	}
	flags := flag.NewFlagSet("fugue-release-domain-dispatch verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var bundleDir string
	flags.StringVar(&bundleDir, "bundle-dir", "", "private authorization bundle directory")
	if err := flags.Parse(args); err != nil {
		return "", "", "", err
	}
	if flags.NArg() != 0 {
		return "", "", "", fmt.Errorf("unexpected positional arguments")
	}
	if err := validateRequiredString("--bundle-dir", bundleDir); err != nil {
		return "", "", "", err
	}
	decision, err := verifyAuthorizationBundle(bundleDir)
	if err != nil {
		return "", "", "", err
	}
	return releasedomain.Outcome(decision.Outcome), releasedomain.Domain(decision.SelectedDomain), decision.PlanDigest, nil
}

func parseAuthorizeFlags(args []string) (authorizeOptions, error) {
	var options authorizeOptions
	if err := rejectDuplicateFlags(args, map[string]bool{
		"ownership": false, "changed-evidence": false, "trusted-base-commit": false,
		"trusted-target-commit": false, "base-canonical-manifest": false,
		"target-canonical-manifest": false, "repeated-target-canonical-manifest": false,
		"argv-snapshot": false, "bundle-dir": false, "release-name": false,
		"release-namespace": false, "base-revision": false, "target-revision": false,
		"binding": true, "ignore-helm-test-hooks": false,
	}); err != nil {
		return authorizeOptions{}, err
	}
	flags := flag.NewFlagSet("fugue-release-domain-dispatch authorize", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.ownershipPath, "ownership", "", "release-domain ownership document")
	flags.StringVar(&options.changedEvidencePath, "changed-evidence", "", "revision-bound changed-file evidence")
	flags.StringVar(&options.trustedBaseCommit, "trusted-base-commit", "", "trusted exact base Git commit")
	flags.StringVar(&options.trustedTargetCommit, "trusted-target-commit", "", "trusted exact target Git commit")
	flags.StringVar(&options.baseManifestPath, "base-canonical-manifest", "", "canonical no-hooks base manifest")
	flags.StringVar(&options.targetManifestPath, "target-canonical-manifest", "", "canonical no-hooks target manifest")
	flags.StringVar(&options.repeatedManifestPath, "repeated-target-canonical-manifest", "", "independently repeated canonical no-hooks target manifest")
	flags.StringVar(&options.argvSnapshotPath, "argv-snapshot", "", "exact private Helm upgrade argv snapshot")
	flags.StringVar(&options.bundleDir, "bundle-dir", "", "fresh private authorization bundle directory")
	flags.StringVar(&options.releaseName, "release-name", "", "exact Helm release name")
	flags.StringVar(&options.releaseNamespace, "release-namespace", "", "exact Helm release namespace")
	flags.StringVar(&options.baseRevision, "base-revision", "", "opaque live/base Helm revision binding")
	flags.StringVar(&options.targetRevision, "target-revision", "", "opaque target Helm revision binding")
	flags.Var(&options.bindings, "binding", "resolved ownership binding key=value (repeatable)")
	flags.Var(&options.ignoreHooks, "ignore-helm-test-hooks", "must be explicitly false; manifests must contain no hooks")
	if err := flags.Parse(args); err != nil {
		return authorizeOptions{}, err
	}
	if flags.NArg() != 0 {
		return authorizeOptions{}, fmt.Errorf("unexpected positional arguments")
	}
	if !options.ignoreHooks.set || options.ignoreHooks.value {
		return authorizeOptions{}, fmt.Errorf("--ignore-helm-test-hooks=false is required")
	}
	for name, value := range map[string]string{
		"--ownership":                          options.ownershipPath,
		"--changed-evidence":                   options.changedEvidencePath,
		"--trusted-base-commit":                options.trustedBaseCommit,
		"--trusted-target-commit":              options.trustedTargetCommit,
		"--base-canonical-manifest":            options.baseManifestPath,
		"--target-canonical-manifest":          options.targetManifestPath,
		"--repeated-target-canonical-manifest": options.repeatedManifestPath,
		"--argv-snapshot":                      options.argvSnapshotPath,
		"--bundle-dir":                         options.bundleDir,
		"--release-name":                       options.releaseName,
		"--release-namespace":                  options.releaseNamespace,
		"--base-revision":                      options.baseRevision,
		"--target-revision":                    options.targetRevision,
	} {
		if err := validateRequiredString(name, value); err != nil {
			return authorizeOptions{}, err
		}
	}
	if err := validateGitCommit(options.trustedBaseCommit); err != nil {
		return authorizeOptions{}, fmt.Errorf("--trusted-base-commit: %w", err)
	}
	if err := validateGitCommit(options.trustedTargetCommit); err != nil {
		return authorizeOptions{}, fmt.Errorf("--trusted-target-commit: %w", err)
	}
	if err := validateRevisionPair(options.baseRevision, options.targetRevision); err != nil {
		return authorizeOptions{}, err
	}
	if options.bindings == nil {
		options.bindings = bindingFlags{}
	}
	for name, value := range map[string]string{
		"releaseName":      options.releaseName,
		"releaseNamespace": options.releaseNamespace,
	} {
		if current, exists := options.bindings[name]; exists && current != value {
			return authorizeOptions{}, fmt.Errorf("--binding %s differs from its trusted release flag", name)
		}
		options.bindings[name] = value
	}
	return options, nil
}

func rejectDuplicateFlags(args []string, policy map[string]bool) error {
	seen := make(map[string]struct{}, len(policy))
	for _, argument := range args {
		if argument == "--" {
			break
		}
		if len(argument) < 2 || argument[0] != '-' {
			continue
		}
		name := strings.TrimLeft(argument, "-")
		name, _, _ = strings.Cut(name, "=")
		repeatable, known := policy[name]
		if !known || repeatable {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("--%s was supplied more than once", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateRevisionPair(base, target string) error {
	parse := func(label, value string, maximum uint64) (uint64, error) {
		parsed, err := strconv.ParseUint(value, 10, 31)
		if err != nil || parsed == 0 || parsed > maximum || strconv.FormatUint(parsed, 10) != value {
			return 0, fmt.Errorf("--%s must be a positive canonical decimal integer no greater than %d", label, maximum)
		}
		return parsed, nil
	}
	baseRevision, err := parse("base-revision", base, 2147483646)
	if err != nil {
		return err
	}
	targetRevision, err := parse("target-revision", target, 2147483647)
	if err != nil {
		return err
	}
	if targetRevision != baseRevision+1 {
		return fmt.Errorf("--target-revision must immediately follow --base-revision")
	}
	return nil
}

func validateRequiredString(name, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", name)
	}
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must be non-empty without surrounding whitespace or NUL", name)
	}
	return nil
}

func buildAuthorization(options authorizeOptions) (authorizationArtifacts, error) {
	ownership, err := readSecureSource(options.ownershipPath, maxOwnershipBytes, false)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("read ownership: %w", err)
	}
	changedEvidence, err := readSecureSource(options.changedEvidencePath, maxChangedEvidenceBytes, false)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("read changed-file evidence: %w", err)
	}
	baseManifest, err := readSecureSource(options.baseManifestPath, maxManifestBytes, true)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("read base canonical manifest: %w", err)
	}
	targetManifest, err := readSecureSource(options.targetManifestPath, maxManifestBytes, true)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("read target canonical manifest: %w", err)
	}
	repeatedManifest, err := readSecureSource(options.repeatedManifestPath, maxManifestBytes, true)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("read repeated target canonical manifest: %w", err)
	}
	argvSnapshot, err := readSecureSource(options.argvSnapshotPath, maxArgvSnapshotBytes, true)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("read upgrade argv snapshot: %w", err)
	}
	if err := validateUpgradeArgvSnapshot(argvSnapshot, options.releaseName, options.releaseNamespace); err != nil {
		return authorizationArtifacts{}, err
	}

	plan, err := releasedomain.BuildPlanFromArtifacts(releasedomain.PlanArtifactInput{
		Ownership:                 ownership,
		ChangedFileEvidence:       changedEvidence,
		TrustedBaseCommit:         options.trustedBaseCommit,
		TrustedTargetCommit:       options.trustedTargetCommit,
		BaseCanonicalManifest:     baseManifest,
		TargetCanonicalManifest:   targetManifest,
		RepeatedCanonicalManifest: repeatedManifest,
		SSoTBaseDigest:            options.baseRevision,
		SSoTTargetDigest:          options.targetRevision,
		SSoTLiveDigest:            options.baseRevision,
		DefaultNamespace:          options.releaseNamespace,
		Bindings:                  map[string]string(options.bindings),
		IgnoreHelmTestHooks:       false,
	})
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("build release-domain plan: %w", err)
	}
	planJSON, err := marshalPrivateJSON(plan)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("encode release-domain plan: %w", err)
	}
	if len(planJSON) > maxPlanBytes {
		return authorizationArtifacts{}, fmt.Errorf("encoded release-domain plan exceeds %d-byte limit", maxPlanBytes)
	}

	artifacts := authorizationArtifacts{
		plan:             plan,
		planJSON:         planJSON,
		ownership:        ownership,
		changedEvidence:  changedEvidence,
		baseManifest:     baseManifest,
		targetManifest:   targetManifest,
		repeatedManifest: repeatedManifest,
		argvSnapshot:     argvSnapshot,
	}
	artifacts.decision = dispatchDecision{
		APIVersion:          decisionAPIVersion,
		Kind:                decisionKind,
		Outcome:             string(plan.Result),
		SelectedDomain:      string(plan.SelectedDomain),
		PlanDigest:          plan.PlanDigest,
		TrustedBaseCommit:   options.trustedBaseCommit,
		TrustedTargetCommit: options.trustedTargetCommit,
		Artifacts: artifactDigests{
			Plan:                   digestBytes(planJSON),
			Ownership:              digestBytes(ownership),
			ChangedFileEvidence:    digestBytes(changedEvidence),
			BaseManifest:           digestBytes(baseManifest),
			TargetManifest:         digestBytes(targetManifest),
			RepeatedTargetManifest: digestBytes(repeatedManifest),
			UpgradeArgvSnapshot:    digestBytes(argvSnapshot),
		},
	}

	if plan.Result != releasedomain.OutcomeSingle {
		return artifacts, nil
	}
	envelope, err := releasedomain.NewTransactionEnvelope(plan, plan.PlanDigest, plan.SelectedDomain)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("construct transaction envelope: %w", err)
	}
	envelopeJSON, err := marshalPrivateJSON(envelope)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("encode transaction envelope: %w", err)
	}
	if len(envelopeJSON) > maxEnvelopeBytes {
		return authorizationArtifacts{}, fmt.Errorf("encoded transaction envelope exceeds %d-byte limit", maxEnvelopeBytes)
	}
	transaction, err := releasedomain.DecodeAndVerifyTransactionEnvelope(bytes.NewReader(envelopeJSON), plan.PlanDigest, plan.SelectedDomain)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("verify transaction envelope: %w", err)
	}
	binding := releasedomain.ExecutionBinding{
		ReleaseName:       options.releaseName,
		ReleaseNamespace:  options.releaseNamespace,
		BaseRevision:      options.baseRevision,
		TargetRevision:    options.targetRevision,
		UpgradeArgvDigest: digestBytes(argvSnapshot),
		HooksPolicy:       releasedomain.HooksPolicyNoHooks,
	}
	execution, err := releasedomain.VerifyRollbackOwnership(releasedomain.RollbackOwnershipInput{
		Transaction:            transaction,
		Binding:                binding,
		Ownership:              ownership,
		BaseManifest:           baseManifest,
		TargetManifest:         targetManifest,
		RepeatedTargetManifest: repeatedManifest,
	})
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("verify rollback ownership: %w", err)
	}
	if err := execution.Verify(); err != nil {
		return authorizationArtifacts{}, fmt.Errorf("verify execution authorization: %w", err)
	}
	bindingJSON, err := marshalPrivateJSON(binding)
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("encode execution binding: %w", err)
	}
	if len(bindingJSON) > maxBindingBytes {
		return authorizationArtifacts{}, fmt.Errorf("encoded execution binding exceeds %d-byte limit", maxBindingBytes)
	}
	rollbackJSON, err := marshalPrivateJSON(execution.Evidence())
	if err != nil {
		return authorizationArtifacts{}, fmt.Errorf("encode rollback ownership evidence: %w", err)
	}
	if len(rollbackJSON) > maxRollbackEvidence {
		return authorizationArtifacts{}, fmt.Errorf("encoded rollback ownership evidence exceeds %d-byte limit", maxRollbackEvidence)
	}
	artifacts.envelopeJSON = envelopeJSON
	artifacts.bindingJSON = bindingJSON
	artifacts.rollbackJSON = rollbackJSON
	artifacts.decision.Artifacts.TransactionEnvelope = digestBytes(envelopeJSON)
	artifacts.decision.Artifacts.ExecutionBinding = digestBytes(bindingJSON)
	artifacts.decision.Artifacts.RollbackOwnershipEvidence = digestBytes(rollbackJSON)
	return artifacts, nil
}

func publishAuthorizationBundle(path string, artifacts authorizationArtifacts) (resultErr error) {
	bundle, err := createFreshPrivateBundle(path)
	if err != nil {
		return fmt.Errorf("create authorization bundle: %w", err)
	}
	defer func() { resultErr = joinErrors(resultErr, bundle.close()) }()
	files := []struct {
		name string
		data []byte
	}{
		{name: ownershipFilename, data: artifacts.ownership},
		{name: changedEvidenceFilename, data: artifacts.changedEvidence},
		{name: baseManifestFilename, data: artifacts.baseManifest},
		{name: targetManifestFilename, data: artifacts.targetManifest},
		{name: repeatedManifestFilename, data: artifacts.repeatedManifest},
		{name: argvSnapshotFilename, data: artifacts.argvSnapshot},
		{name: planFilename, data: artifacts.planJSON},
	}
	if artifacts.plan.Result == releasedomain.OutcomeSingle {
		files = append(files,
			struct {
				name string
				data []byte
			}{name: envelopeFilename, data: artifacts.envelopeJSON},
			struct {
				name string
				data []byte
			}{name: executionBindingFilename, data: artifacts.bindingJSON},
			struct {
				name string
				data []byte
			}{name: rollbackEvidenceFilename, data: artifacts.rollbackJSON},
		)
	}
	for _, file := range files {
		if err := bundle.writeAtomic(file.name, file.data); err != nil {
			return fmt.Errorf("publish %s: %w", file.name, err)
		}
	}
	decisionJSON, err := marshalPrivateJSON(artifacts.decision)
	if err != nil {
		return fmt.Errorf("encode decision: %w", err)
	}
	if len(decisionJSON) > maxDecisionBytes {
		return fmt.Errorf("encoded decision exceeds limit")
	}
	// decision.json is the bundle completion marker and is always published last.
	if err := bundle.writeAtomic(decisionFilename, decisionJSON); err != nil {
		return fmt.Errorf("publish decision: %w", err)
	}
	if err := bundle.verifyStable(expectedBundleFiles(artifacts.plan.Result)); err != nil {
		return fmt.Errorf("verify published authorization bundle: %w", err)
	}
	return nil
}

func verifyAuthorizationBundle(path string) (dispatchDecision, error) {
	return verifyAuthorizationBundleInto(path, nil)
}

func inspectAuthorizationBundle(path string) (verifiedAuthorizationBundle, error) {
	var details verifiedAuthorizationBundle
	if _, err := verifyAuthorizationBundleInto(path, &details); err != nil {
		return verifiedAuthorizationBundle{}, err
	}
	return details, nil
}

func verifyAuthorizationBundleInto(path string, details *verifiedAuthorizationBundle) (verified dispatchDecision, resultErr error) {
	bundle, err := openPrivateBundle(path)
	if err != nil {
		return dispatchDecision{}, fmt.Errorf("open authorization bundle: %w", err)
	}
	defer func() {
		if closeErr := bundle.close(); closeErr != nil {
			verified = dispatchDecision{}
			resultErr = joinErrors(resultErr, fmt.Errorf("close authorization bundle: %w", closeErr))
		}
	}()
	decisionJSON, err := bundle.read(decisionFilename, maxDecisionBytes)
	if err != nil {
		return dispatchDecision{}, fmt.Errorf("read decision: %w", err)
	}
	var decision dispatchDecision
	if err := decodeStrictJSON(decisionJSON, &decision); err != nil {
		return dispatchDecision{}, fmt.Errorf("decode decision: %w", err)
	}
	if err := validateDecision(decision); err != nil {
		return dispatchDecision{}, fmt.Errorf("validate decision: %w", err)
	}
	outcome := releasedomain.Outcome(decision.Outcome)
	if err := bundle.verifyNames(expectedBundleFiles(outcome)); err != nil {
		return dispatchDecision{}, err
	}

	planJSON, err := bundle.read(planFilename, maxPlanBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	ownership, err := bundle.read(ownershipFilename, maxOwnershipBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	changedEvidence, err := bundle.read(changedEvidenceFilename, maxChangedEvidenceBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	baseManifest, err := bundle.read(baseManifestFilename, maxManifestBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	targetManifest, err := bundle.read(targetManifestFilename, maxManifestBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	repeatedManifest, err := bundle.read(repeatedManifestFilename, maxManifestBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	argvSnapshot, err := bundle.read(argvSnapshotFilename, maxArgvSnapshotBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	common := map[string][]byte{
		"plan":                     planJSON,
		"ownership":                ownership,
		"changed-file evidence":    changedEvidence,
		"base manifest":            baseManifest,
		"target manifest":          targetManifest,
		"repeated target manifest": repeatedManifest,
		"upgrade argv snapshot":    argvSnapshot,
	}
	expectedCommon := map[string]string{
		"plan":                     decision.Artifacts.Plan,
		"ownership":                decision.Artifacts.Ownership,
		"changed-file evidence":    decision.Artifacts.ChangedFileEvidence,
		"base manifest":            decision.Artifacts.BaseManifest,
		"target manifest":          decision.Artifacts.TargetManifest,
		"repeated target manifest": decision.Artifacts.RepeatedTargetManifest,
		"upgrade argv snapshot":    decision.Artifacts.UpgradeArgvSnapshot,
	}
	for name, data := range common {
		if digestBytes(data) != expectedCommon[name] {
			return dispatchDecision{}, fmt.Errorf("%s artifact digest mismatch", name)
		}
	}

	plan, err := releasedomain.DecodeAndVerifyPlan(bytes.NewReader(planJSON), decision.PlanDigest)
	if err != nil {
		return dispatchDecision{}, fmt.Errorf("verify persisted plan: %w", err)
	}
	if string(plan.Result) != decision.Outcome || string(plan.SelectedDomain) != decision.SelectedDomain {
		return dispatchDecision{}, fmt.Errorf("decision outcome differs from persisted plan")
	}
	if plan.Digests.ClassificationContext.IgnoreHelmTestHooks {
		return dispatchDecision{}, fmt.Errorf("persisted plan does not enforce canonical no-hooks manifests")
	}
	rebuilt, err := releasedomain.BuildPlanFromArtifacts(releasedomain.PlanArtifactInput{
		Ownership:                 ownership,
		ChangedFileEvidence:       changedEvidence,
		TrustedBaseCommit:         decision.TrustedBaseCommit,
		TrustedTargetCommit:       decision.TrustedTargetCommit,
		BaseCanonicalManifest:     baseManifest,
		TargetCanonicalManifest:   targetManifest,
		RepeatedCanonicalManifest: repeatedManifest,
		SSoTBaseDigest:            plan.Digests.Base,
		SSoTTargetDigest:          plan.Digests.Target,
		SSoTLiveDigest:            plan.Digests.Live,
		DefaultNamespace:          plan.Digests.ClassificationContext.DefaultNamespace,
		Bindings:                  plan.Digests.ClassificationContext.BindingMap(),
		IgnoreHelmTestHooks:       plan.Digests.ClassificationContext.IgnoreHelmTestHooks,
	})
	if err != nil {
		return dispatchDecision{}, fmt.Errorf("rebuild release-domain plan: %w", err)
	}
	if rebuilt.PlanDigest != plan.PlanDigest {
		return dispatchDecision{}, fmt.Errorf("rebuilt release-domain plan differs from persisted plan")
	}
	bindings := plan.Digests.ClassificationContext.BindingMap()
	if err := validateUpgradeArgvSnapshot(argvSnapshot, bindings["releaseName"], bindings["releaseNamespace"]); err != nil {
		return dispatchDecision{}, err
	}

	if outcome != releasedomain.OutcomeSingle {
		if err := bundle.verifyStable(expectedBundleFiles(outcome)); err != nil {
			return dispatchDecision{}, err
		}
		if details != nil {
			*details = verifiedAuthorizationBundle{Decision: decision, Plan: plan}
		}
		return decision, nil
	}

	envelopeJSON, err := bundle.read(envelopeFilename, maxEnvelopeBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	bindingJSON, err := bundle.read(executionBindingFilename, maxBindingBytes)
	if err != nil {
		return dispatchDecision{}, err
	}
	rollbackJSON, err := bundle.read(rollbackEvidenceFilename, maxRollbackEvidence)
	if err != nil {
		return dispatchDecision{}, err
	}
	for _, artifact := range []struct {
		name     string
		data     []byte
		expected string
	}{
		{name: "transaction envelope", data: envelopeJSON, expected: decision.Artifacts.TransactionEnvelope},
		{name: "execution binding", data: bindingJSON, expected: decision.Artifacts.ExecutionBinding},
		{name: "rollback ownership evidence", data: rollbackJSON, expected: decision.Artifacts.RollbackOwnershipEvidence},
	} {
		if digestBytes(artifact.data) != artifact.expected {
			return dispatchDecision{}, fmt.Errorf("%s artifact digest mismatch", artifact.name)
		}
	}
	domain, err := releasedomain.ParseDomain(decision.SelectedDomain)
	if err != nil {
		return dispatchDecision{}, err
	}
	transaction, err := releasedomain.DecodeAndVerifyTransactionEnvelope(bytes.NewReader(envelopeJSON), decision.PlanDigest, domain)
	if err != nil {
		return dispatchDecision{}, fmt.Errorf("verify transaction envelope: %w", err)
	}
	var binding releasedomain.ExecutionBinding
	if err := decodeStrictJSON(bindingJSON, &binding); err != nil {
		return dispatchDecision{}, fmt.Errorf("decode execution binding: %w", err)
	}
	if binding.UpgradeArgvDigest != digestBytes(argvSnapshot) || binding.HooksPolicy != releasedomain.HooksPolicyNoHooks {
		return dispatchDecision{}, fmt.Errorf("execution binding differs from no-hooks argv boundary")
	}
	var persistedRollback releasedomain.RollbackOwnershipEvidence
	if err := decodeStrictJSON(rollbackJSON, &persistedRollback); err != nil {
		return dispatchDecision{}, fmt.Errorf("decode rollback ownership evidence: %w", err)
	}
	execution, err := releasedomain.VerifyRollbackOwnership(releasedomain.RollbackOwnershipInput{
		Transaction:            transaction,
		Binding:                binding,
		Ownership:              ownership,
		BaseManifest:           baseManifest,
		TargetManifest:         targetManifest,
		RepeatedTargetManifest: repeatedManifest,
	})
	if err != nil {
		return dispatchDecision{}, fmt.Errorf("reverify rollback ownership: %w", err)
	}
	if err := execution.Verify(); err != nil {
		return dispatchDecision{}, fmt.Errorf("verify reconstructed execution authorization: %w", err)
	}
	if execution.Domain() != domain || execution.PlanDigest() != decision.PlanDigest || !reflect.DeepEqual(execution.Binding(), binding) || !reflect.DeepEqual(execution.Evidence(), persistedRollback) {
		return dispatchDecision{}, fmt.Errorf("reconstructed execution authorization differs from persisted bundle")
	}
	if err := bundle.verifyStable(expectedBundleFiles(outcome)); err != nil {
		return dispatchDecision{}, err
	}
	if details != nil {
		bindingCopy := binding
		rollbackCopy := persistedRollback
		*details = verifiedAuthorizationBundle{
			Decision:         decision,
			Plan:             plan,
			ExecutionBinding: &bindingCopy,
			RollbackEvidence: &rollbackCopy,
		}
	}
	return decision, nil
}

func validateDecision(decision dispatchDecision) error {
	if decision.APIVersion != decisionAPIVersion || decision.Kind != decisionKind {
		return fmt.Errorf("unsupported decision identity")
	}
	if err := validateCanonicalDigest(decision.PlanDigest); err != nil {
		return fmt.Errorf("plan digest: %w", err)
	}
	if err := validateGitCommit(decision.TrustedBaseCommit); err != nil {
		return fmt.Errorf("trusted base commit: %w", err)
	}
	if err := validateGitCommit(decision.TrustedTargetCommit); err != nil {
		return fmt.Errorf("trusted target commit: %w", err)
	}
	outcome := releasedomain.Outcome(decision.Outcome)
	switch outcome {
	case releasedomain.OutcomeSingle:
		if _, err := releasedomain.ParseDomain(decision.SelectedDomain); err != nil {
			return err
		}
	case releasedomain.OutcomeZero, releasedomain.OutcomeMultiple, releasedomain.OutcomeUnknown:
		if decision.SelectedDomain != "" {
			return fmt.Errorf("non-single decision selected a domain")
		}
	default:
		return fmt.Errorf("unsupported decision outcome")
	}
	common := []string{
		decision.Artifacts.Plan,
		decision.Artifacts.Ownership,
		decision.Artifacts.ChangedFileEvidence,
		decision.Artifacts.BaseManifest,
		decision.Artifacts.TargetManifest,
		decision.Artifacts.RepeatedTargetManifest,
		decision.Artifacts.UpgradeArgvSnapshot,
	}
	for _, digest := range common {
		if err := validateCanonicalDigest(digest); err != nil {
			return fmt.Errorf("common artifact digest: %w", err)
		}
	}
	execution := []string{
		decision.Artifacts.TransactionEnvelope,
		decision.Artifacts.ExecutionBinding,
		decision.Artifacts.RollbackOwnershipEvidence,
	}
	for _, digest := range execution {
		if outcome == releasedomain.OutcomeSingle {
			if err := validateCanonicalDigest(digest); err != nil {
				return fmt.Errorf("execution artifact digest: %w", err)
			}
		} else if digest != "" {
			return fmt.Errorf("non-single decision contains execution authorization")
		}
	}
	return nil
}

func expectedBundleFiles(outcome releasedomain.Outcome) map[string]struct{} {
	files := map[string]struct{}{
		decisionFilename:         {},
		planFilename:             {},
		ownershipFilename:        {},
		changedEvidenceFilename:  {},
		baseManifestFilename:     {},
		targetManifestFilename:   {},
		repeatedManifestFilename: {},
		argvSnapshotFilename:     {},
	}
	if outcome == releasedomain.OutcomeSingle {
		files[envelopeFilename] = struct{}{}
		files[executionBindingFilename] = struct{}{}
		files[rollbackEvidenceFilename] = struct{}{}
	}
	return files
}

func marshalPrivateJSON(value any) ([]byte, error) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validateCanonicalDigest(value string) error {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return fmt.Errorf("must be canonical lowercase sha256")
	}
	for _, digit := range value[len("sha256:"):] {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return fmt.Errorf("must be canonical lowercase sha256")
		}
	}
	return nil
}

func validateGitCommit(value string) error {
	if len(value) != 40 {
		return fmt.Errorf("must be exact lowercase 40-hex")
	}
	for _, digit := range value {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return fmt.Errorf("must be exact lowercase 40-hex")
		}
	}
	return nil
}

// validateUpgradeArgvSnapshot defines the only executable representation
// authorized by this command. After verify succeeds, production must decode
// and execute this exact NUL-delimited file from the verified private bundle;
// it must never rebuild or substitute an in-memory argv.
func validateUpgradeArgvSnapshot(snapshot []byte, releaseName, releaseNamespace string) error {
	if len(snapshot) == 0 || snapshot[len(snapshot)-1] != 0 {
		return fmt.Errorf("upgrade argv snapshot must be non-empty and NUL terminated")
	}
	encoded := bytes.Split(snapshot[:len(snapshot)-1], []byte{0})
	if len(encoded) < 8 {
		return fmt.Errorf("upgrade argv snapshot is incomplete")
	}
	arguments := make([]string, len(encoded))
	for index, argument := range encoded {
		if len(argument) == 0 {
			return fmt.Errorf("upgrade argv snapshot contains an empty argument")
		}
		if !utf8.Valid(argument) {
			return fmt.Errorf("upgrade argv snapshot argument %d is not valid UTF-8", index)
		}
		arguments[index] = string(argument)
	}
	if arguments[0] != "helm" || arguments[1] != "upgrade" {
		return fmt.Errorf("upgrade argv snapshot must begin with helm upgrade")
	}
	if arguments[2] != releaseName || strings.HasPrefix(arguments[2], "-") {
		return fmt.Errorf("upgrade argv snapshot release name differs from execution binding")
	}
	if strings.HasPrefix(arguments[3], "-") {
		return fmt.Errorf("upgrade argv snapshot chart must be a non-flag argument")
	}
	if arguments[4] != "-n" || arguments[5] != releaseNamespace {
		return fmt.Errorf("upgrade argv snapshot namespace prefix differs from execution binding")
	}
	if arguments[6] != "--reset-then-reuse-values" || arguments[7] != "--no-hooks" {
		return fmt.Errorf("upgrade argv snapshot protected flags are not in the fixed safe prefix")
	}
	for index := 8; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "-f" {
			if index+1 >= len(arguments) || strings.HasPrefix(arguments[index+1], "-") {
				return fmt.Errorf("upgrade argv snapshot values file must be a non-flag argument")
			}
			index++
			continue
		}
		if strings.HasPrefix(argument, "-") && !strings.HasPrefix(argument, "--") {
			return fmt.Errorf("upgrade argv snapshot contains a non-canonical shorthand option")
		}
		if argument == "--" ||
			argument == "--no-hooks" ||
			argument == "-n" ||
			argument == "--namespace" ||
			argument == "--reuse-values" ||
			argument == "--reset-values" ||
			strings.HasPrefix(argument, "--no-hooks=") ||
			strings.HasPrefix(argument, "--namespace=") ||
			strings.HasPrefix(argument, "--reuse-values=") ||
			strings.HasPrefix(argument, "--reset-values=") ||
			strings.HasPrefix(argument, "--reset-then-reuse-values") {
			return fmt.Errorf("upgrade argv snapshot contains a protected option outside the fixed safe prefix")
		}
	}
	return nil
}

func joinErrors(left, right error) error {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return fmt.Errorf("%v; %w", left, right)
}
