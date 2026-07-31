package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"fugue/internal/releasedomain"
)

const maxInputBytes = 32 << 20

type bindingFlags map[string]string

func (values *bindingFlags) String() string { return "" }
func (values *bindingFlags) Set(value string) error {
	key, resolved, ok := strings.Cut(value, "=")
	if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(resolved) == "" {
		return fmt.Errorf("binding must be key=value")
	}
	if *values == nil {
		*values = bindingFlags{}
	}
	if _, duplicate := (*values)[key]; duplicate {
		return fmt.Errorf("binding is duplicated")
	}
	(*values)[key] = resolved
	return nil
}

type commonFlags struct {
	evidenceDir string
	releaseName string
	namespace   string
	fullname    string
	source      string
	baseRev     string
	targetRev   string
	bindings    bindingFlags
}

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = io.WriteString(stderr, "fugue-public-data-plane-adoption: subcommand is required\n")
		return 1
	}
	var err error
	switch args[0] {
	case "canonicalize":
		err = runCanonicalize(args[1:], stdin, stdout)
	case "intent":
		err = runIntent(args[1:], stdout)
	case "post-render":
		err = runPostRender(args[1:], stdin, stdout)
	case "transaction-post-render":
		err = runTransactionPostRender(args[1:], stdin, stdout)
	case "authorize":
		err = runAuthorize(args[1:], stdout)
	case "verify-prewrite":
		err = runVerifyPrewrite(args[1:])
	case "restore-patches":
		err = runRestorePatches(args[1:], stdout)
	case "verify-restore":
		err = runVerifyRestore(args[1:])
	case "verify-recovery-candidate":
		err = runVerifyRecoveryCandidate(args[1:])
	case "finalize":
		err = runFinalize(args[1:], stdout)
	case "verify-stage2":
		err = runVerifyStage2(args[1:])
	case "verify-recovery-base":
		err = runVerifyRecoveryBase(args[1:])
	case "trace":
		err = runTrace(args[1:], stdout)
	case "wal-init":
		err = runWALInit(args[1:], stdout)
	case "wal-advance":
		err = runWALAdvance(args[1:], stdout)
	case "wal-verify":
		err = runWALVerify(args[1:], stdout)
	default:
		err = fmt.Errorf("unsupported subcommand")
	}
	if err != nil {
		_, _ = io.WriteString(stderr, "fugue-public-data-plane-adoption: "+err.Error()+"\n")
		return 1
	}
	return 0
}

func runCanonicalize(args []string, stdin io.Reader, stdout io.Writer) error {
	flags := flag.NewFlagSet("canonicalize", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var ownershipPath, namespace string
	flags.StringVar(&ownershipPath, "ownership", "", "ownership file")
	flags.StringVar(&namespace, "namespace", "", "namespace")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || ownershipPath == "" || namespace == "" {
		return fmt.Errorf("flags are invalid")
	}
	ownership, err := readLimitedFile(ownershipPath)
	if err != nil {
		return err
	}
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return fmt.Errorf("ownership is invalid")
	}
	rendered, err := io.ReadAll(io.LimitReader(stdin, maxInputBytes+1))
	if err != nil || len(rendered) > maxInputBytes {
		return fmt.Errorf("rendered manifest is invalid")
	}
	canonical, err := releasedomain.CanonicalizeRenderedManifest(rendered, spec, namespace)
	if err != nil {
		return fmt.Errorf("canonicalization failed: %w", err)
	}
	_, err = stdout.Write(canonical)
	return err
}

func addCommon(flags *flag.FlagSet, options *commonFlags) {
	flags.StringVar(&options.evidenceDir, "evidence-dir", "", "private evidence directory")
	flags.StringVar(&options.releaseName, "release", "", "Helm release name")
	flags.StringVar(&options.namespace, "namespace", "", "Helm release namespace")
	flags.StringVar(&options.fullname, "fullname", "", "Helm release fullname")
	flags.StringVar(&options.source, "source-commit", "", "exact source commit")
	flags.StringVar(&options.baseRev, "base-revision", "", "base Helm revision")
	flags.StringVar(&options.targetRev, "target-revision", "", "target Helm revision")
	flags.Var(&options.bindings, "binding", "resolved ownership binding")
}

func parseCommon(name string, args []string, revisions bool) (commonFlags, error) {
	var options commonFlags
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	addCommon(flags, &options)
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return commonFlags{}, fmt.Errorf("flags are invalid")
	}
	for label, value := range map[string]string{
		"evidence directory": options.evidenceDir, "release": options.releaseName,
		"namespace": options.namespace, "fullname": options.fullname, "source commit": options.source,
	} {
		if strings.TrimSpace(value) == "" {
			return commonFlags{}, fmt.Errorf("%s is required", label)
		}
	}
	if revisions && (options.baseRev == "" || options.targetRev == "") {
		return commonFlags{}, fmt.Errorf("base and target revisions are required")
	}
	return options, nil
}

func runIntent(args []string, stdout io.Writer) error {
	options, err := parseCommon("intent", args, false)
	if err != nil {
		return err
	}
	snapshot, err := readEvidence(options.evidenceDir, "snapshot.json")
	if err != nil {
		return err
	}
	intent, err := releasedomain.BuildPublicDataPlaneAdoptionIntent(
		snapshot, options.source, options.releaseName, options.namespace, options.fullname,
	)
	if err != nil {
		return fmt.Errorf("intent construction failed: %w", err)
	}
	if err := verifyGitAncestry(intent.Record.GitSHA, intent.SourceCommit); err != nil {
		return err
	}
	return writeJSON(stdout, intent)
}

func runPostRender(args []string, stdin io.Reader, stdout io.Writer) error {
	flags := flag.NewFlagSet("post-render", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var ownershipPath, intentPath, namespace string
	flags.StringVar(&ownershipPath, "ownership", "", "ownership file")
	flags.StringVar(&intentPath, "intent", "", "intent file")
	flags.StringVar(&namespace, "namespace", "", "namespace")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || ownershipPath == "" || intentPath == "" || namespace == "" {
		return fmt.Errorf("flags are invalid")
	}
	ownership, err := readLimitedFile(ownershipPath)
	if err != nil {
		return err
	}
	var intent releasedomain.PublicDataPlaneAdoptionIntent
	if err := readStrictJSONFile(intentPath, &intent); err != nil {
		return err
	}
	rendered, err := io.ReadAll(io.LimitReader(stdin, maxInputBytes+1))
	if err != nil || len(rendered) > maxInputBytes {
		return fmt.Errorf("rendered manifest is invalid")
	}
	target, err := releasedomain.RenderPublicDataPlaneAdoptionTarget(rendered, ownership, namespace, intent)
	if err != nil {
		return fmt.Errorf("post-render failed: %w", err)
	}
	_, err = stdout.Write(target)
	return err
}

func runTransactionPostRender(args []string, stdin io.Reader, stdout io.Writer) error {
	flags := flag.NewFlagSet("transaction-post-render", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var ownershipPath, transactionPath, namespace string
	flags.StringVar(&ownershipPath, "ownership", "", "ownership file")
	flags.StringVar(&transactionPath, "transaction", "", "transaction envelope")
	flags.StringVar(&namespace, "namespace", "", "namespace")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || ownershipPath == "" || transactionPath == "" || namespace == "" {
		return fmt.Errorf("flags are invalid")
	}
	ownership, err := readLimitedFile(ownershipPath)
	if err != nil {
		return err
	}
	var envelope releasedomain.PublicDataPlaneAdoptionTransactionEnvelope
	if err := readStrictJSONFile(transactionPath, &envelope); err != nil {
		return err
	}
	rendered, err := io.ReadAll(io.LimitReader(stdin, maxInputBytes+1))
	if err != nil || len(rendered) > maxInputBytes {
		return fmt.Errorf("rendered manifest is invalid")
	}
	target, err := releasedomain.RenderPublicDataPlaneAdoptionTransactionTarget(
		rendered, ownership, namespace, envelope,
	)
	if err != nil {
		return fmt.Errorf("transaction post-render failed: %w", err)
	}
	_, err = stdout.Write(target)
	return err
}

func runAuthorize(args []string, stdout io.Writer) error {
	options, err := parseCommon("authorize", args, true)
	if err != nil {
		return err
	}
	input, err := loadInput(options, "snapshot.json")
	if err != nil {
		return err
	}
	plan, restore, err := releasedomain.BuildPublicDataPlaneAdoptionPlan(input)
	if err != nil {
		return fmt.Errorf("plan construction failed: %w", err)
	}
	if err := verifyGitAncestry(plan.Intent.Record.GitSHA, plan.SourceCommit); err != nil {
		return err
	}
	envelope, err := releasedomain.NewPublicDataPlaneAdoptionTransaction(plan)
	if err != nil {
		return err
	}
	if err := writePrivateJSON(filepath.Join(options.evidenceDir, "transaction.json"), envelope); err != nil {
		return err
	}
	if err := writePrivateFile(filepath.Join(options.evidenceDir, "restore.json"), append(restore, '\n')); err != nil {
		return err
	}
	return writeJSON(stdout, envelope)
}

func runVerifyPrewrite(args []string) error {
	options, err := parseCommon("verify-prewrite", args, true)
	if err != nil {
		return err
	}
	var envelope releasedomain.PublicDataPlaneAdoptionTransactionEnvelope
	if err := readStrictJSONFile(filepath.Join(options.evidenceDir, "transaction.json"), &envelope); err != nil {
		return err
	}
	if err := releasedomain.VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil {
		return err
	}
	restore, err := readEvidence(options.evidenceDir, "restore.json")
	if err != nil {
		return err
	}
	input, err := loadInputSet(options, "prewrite-")
	if err != nil {
		return err
	}
	if err := verifyGitAncestry(envelope.Plan.Intent.Record.GitSHA, envelope.Plan.SourceCommit); err != nil {
		return err
	}
	return releasedomain.VerifyPublicDataPlaneAdoptionPrewrite(envelope.Plan, bytes.TrimSpace(restore), input)
}

func runRestorePatches(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("restore-patches", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var restorePath string
	flags.StringVar(&restorePath, "restore", "", "restore file")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || restorePath == "" {
		return fmt.Errorf("flags are invalid")
	}
	restore, err := readLimitedFile(restorePath)
	if err != nil {
		return err
	}
	patches, err := releasedomain.PublicDataPlaneAdoptionRestorePatches(bytes.TrimSpace(restore))
	if err != nil {
		return err
	}
	return writeJSON(stdout, patches)
}

func runVerifyRestore(args []string) error {
	flags := flag.NewFlagSet("verify-restore", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var restorePath, snapshotPath, releaseName, namespace, fullname string
	flags.StringVar(&restorePath, "restore", "", "restore file")
	flags.StringVar(&snapshotPath, "snapshot", "", "fresh snapshot")
	flags.StringVar(&releaseName, "release", "", "release")
	flags.StringVar(&namespace, "namespace", "", "namespace")
	flags.StringVar(&fullname, "fullname", "", "fullname")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || restorePath == "" || snapshotPath == "" ||
		releaseName == "" || namespace == "" || fullname == "" {
		return fmt.Errorf("flags are invalid")
	}
	restore, err := readLimitedFile(restorePath)
	if err != nil {
		return err
	}
	snapshot, err := readLimitedFile(snapshotPath)
	if err != nil {
		return err
	}
	return releasedomain.VerifyPublicDataPlaneAdoptionRestore(bytes.TrimSpace(restore), snapshot, releaseName, namespace, fullname)
}

func runVerifyRecoveryCandidate(args []string) error {
	flags := flag.NewFlagSet("verify-recovery-candidate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var restorePath, snapshotPath, transactionPath, namespace string
	flags.StringVar(&restorePath, "restore", "", "restore witness")
	flags.StringVar(&snapshotPath, "snapshot", "", "fresh snapshot")
	flags.StringVar(&transactionPath, "transaction", "", "transaction envelope")
	flags.StringVar(&namespace, "namespace", "", "namespace")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || restorePath == "" || snapshotPath == "" || transactionPath == "" || namespace == "" {
		return fmt.Errorf("flags are invalid")
	}
	var envelope releasedomain.PublicDataPlaneAdoptionTransactionEnvelope
	if err := readStrictJSONFile(transactionPath, &envelope); err != nil {
		return err
	}
	if err := releasedomain.VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil {
		return err
	}
	restore, err := readLimitedFile(restorePath)
	if err != nil {
		return err
	}
	snapshot, err := readLimitedFile(snapshotPath)
	if err != nil {
		return err
	}
	return releasedomain.VerifyPublicDataPlaneAdoptionRecoveryCandidate(
		bytes.TrimSpace(restore), snapshot, namespace, envelope.Plan.Intent.TargetEdgeImageRef,
	)
}

func runFinalize(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("finalize", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var evidenceDir, revision string
	flags.StringVar(&evidenceDir, "evidence-dir", "", "evidence directory")
	flags.StringVar(&revision, "revision", "", "live Helm revision")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || evidenceDir == "" || revision == "" {
		return fmt.Errorf("flags are invalid")
	}
	var envelope releasedomain.PublicDataPlaneAdoptionTransactionEnvelope
	if err := readStrictJSONFile(filepath.Join(evidenceDir, "transaction.json"), &envelope); err != nil {
		return err
	}
	current, err := readEvidence(evidenceDir, "final-manifest.yaml")
	if err != nil {
		return err
	}
	snapshot, err := readEvidence(evidenceDir, "final-snapshot.json")
	if err != nil {
		return err
	}
	observed, err := readEvidence(evidenceDir, "final-observed.yaml")
	if err != nil {
		return err
	}
	ownership, err := readEvidence(evidenceDir, "ownership.yaml")
	if err != nil {
		return err
	}
	baseline, err := releasedomain.FinalizePublicDataPlaneAdoptionBaseline(
		envelope.Plan, revision, current, snapshot, observed, ownership,
	)
	if err != nil {
		return err
	}
	if err := writePrivateJSON(filepath.Join(evidenceDir, "stage1-baseline.json"), baseline); err != nil {
		return err
	}
	return writeJSON(stdout, baseline)
}

func runVerifyStage2(args []string) error {
	flags := flag.NewFlagSet("verify-stage2", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var baselinePath, tracePath, manifestPath, revision string
	flags.StringVar(&baselinePath, "baseline", "", "Stage1 baseline")
	flags.StringVar(&tracePath, "trace", "", "completed Stage1 execution trace")
	flags.StringVar(&manifestPath, "current-manifest", "", "current Helm manifest")
	flags.StringVar(&revision, "current-revision", "", "current Helm revision")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || baselinePath == "" || tracePath == "" || manifestPath == "" || revision == "" {
		return fmt.Errorf("flags are invalid")
	}
	var baseline releasedomain.PublicDataPlaneAdoptionBaseline
	if err := readStrictJSONFile(baselinePath, &baseline); err != nil {
		return err
	}
	var trace releasedomain.PublicDataPlaneAdoptionExecutionTrace
	if err := readStrictJSONFile(tracePath, &trace); err != nil {
		return err
	}
	manifest, err := readLimitedFile(manifestPath)
	if err != nil {
		return err
	}
	return releasedomain.VerifyPublicDataPlaneStage2Handoff(baseline, trace, revision, manifest)
}

func runVerifyRecoveryBase(args []string) error {
	flags := flag.NewFlagSet("verify-recovery-base", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var transactionPath, manifestPath, revision string
	flags.StringVar(&transactionPath, "transaction", "", "transaction envelope")
	flags.StringVar(&manifestPath, "current-manifest", "", "current canonical Helm manifest")
	flags.StringVar(&revision, "current-revision", "", "current Helm revision")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || transactionPath == "" || manifestPath == "" || revision == "" {
		return fmt.Errorf("flags are invalid")
	}
	var envelope releasedomain.PublicDataPlaneAdoptionTransactionEnvelope
	if err := readStrictJSONFile(transactionPath, &envelope); err != nil {
		return err
	}
	manifest, err := readLimitedFile(manifestPath)
	if err != nil {
		return err
	}
	return releasedomain.VerifyPublicDataPlaneAdoptionRecoveryBase(envelope, revision, manifest)
}

func runTrace(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("trace", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var transactionPath, tracePath, phase, at string
	flags.StringVar(&transactionPath, "transaction", "", "transaction envelope")
	flags.StringVar(&tracePath, "trace", "", "execution trace")
	flags.StringVar(&phase, "phase", "", "next execution phase")
	flags.StringVar(&at, "at", "", "RFC3339Nano event time")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || transactionPath == "" || tracePath == "" || phase == "" || at == "" {
		return fmt.Errorf("flags are invalid")
	}
	var envelope releasedomain.PublicDataPlaneAdoptionTransactionEnvelope
	if err := readStrictJSONFile(transactionPath, &envelope); err != nil {
		return err
	}
	if err := releasedomain.VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil {
		return err
	}
	var trace releasedomain.PublicDataPlaneAdoptionExecutionTrace
	if info, err := os.Lstat(tracePath); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("execution trace path is invalid")
		}
		if err := readStrictJSONFile(tracePath, &trace); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("execution trace path is invalid")
	}
	updated, err := releasedomain.AppendPublicDataPlaneAdoptionTrace(trace, envelope.Plan.Digest, phase, at)
	if err != nil {
		return err
	}
	if err := writePrivateJSON(tracePath, updated); err != nil {
		return err
	}
	return writeJSON(stdout, updated)
}

type walFlags struct {
	transactionPath string
	restorePath     string
	walPath         string
	leaseNamespace  string
	leaseName       string
	leaseOwner      string
	leaseToken      string
	at              string
}

func addWALArtifactFlags(flags *flag.FlagSet, options *walFlags) {
	flags.StringVar(&options.transactionPath, "transaction", "", "transaction envelope")
	flags.StringVar(&options.restorePath, "restore", "", "narrow restore witness")
	flags.StringVar(&options.walPath, "wal", "", "recovery WAL")
}

func runWALInit(args []string, stdout io.Writer) error {
	var options walFlags
	flags := flag.NewFlagSet("wal-init", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	addWALArtifactFlags(flags, &options)
	flags.StringVar(&options.leaseNamespace, "lease-namespace", "", "Lease namespace")
	flags.StringVar(&options.leaseName, "lease-name", "", "Lease name")
	flags.StringVar(&options.leaseOwner, "lease-owner", "", "Lease owner")
	flags.StringVar(&options.leaseToken, "lease-token", "", "Lease fencing token")
	flags.StringVar(&options.at, "at", "", "RFC3339Nano time")
	var originRunID string
	var originRunAttempt int
	flags.StringVar(&originRunID, "origin-run-id", "", "origin Actions run ID")
	flags.IntVar(&originRunAttempt, "origin-run-attempt", 0, "origin Actions run attempt")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || options.transactionPath == "" || options.restorePath == "" ||
		options.walPath == "" || options.leaseNamespace == "" || options.leaseName == "" || options.leaseOwner == "" ||
		options.leaseToken == "" || options.at == "" || originRunID == "" || originRunAttempt == 0 {
		return fmt.Errorf("flags are invalid")
	}
	transaction, restore, envelope, err := readRecoveryArtifacts(options.transactionPath, options.restorePath)
	if err != nil {
		return err
	}
	wal, err := releasedomain.NewPublicDataPlaneAdoptionRecoveryWAL(
		envelope, transaction, bytes.TrimSpace(restore), options.leaseNamespace, options.leaseName,
		options.leaseOwner, options.leaseToken, options.at,
		originRunID, originRunAttempt,
	)
	if err != nil {
		return err
	}
	if err := writePrivateJSON(options.walPath, wal); err != nil {
		return err
	}
	return writeJSON(stdout, wal)
}

func runWALAdvance(args []string, stdout io.Writer) error {
	var options walFlags
	flags := flag.NewFlagSet("wal-advance", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	addWALArtifactFlags(flags, &options)
	var phase, baselineDigest string
	flags.StringVar(&options.leaseOwner, "lease-owner", "", "Lease owner")
	flags.StringVar(&options.leaseToken, "lease-token", "", "Lease fencing token")
	flags.StringVar(&phase, "phase", "", "next recovery phase")
	flags.StringVar(&options.at, "at", "", "RFC3339Nano time")
	flags.StringVar(&baselineDigest, "baseline-digest", "", "final baseline digest")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || options.transactionPath == "" || options.restorePath == "" ||
		options.walPath == "" || options.leaseOwner == "" || options.leaseToken == "" || phase == "" || options.at == "" {
		return fmt.Errorf("flags are invalid")
	}
	transaction, restore, _, err := readRecoveryArtifacts(options.transactionPath, options.restorePath)
	if err != nil {
		return err
	}
	var wal releasedomain.PublicDataPlaneAdoptionRecoveryWAL
	if err := readStrictJSONFile(options.walPath, &wal); err != nil {
		return err
	}
	if err := releasedomain.VerifyPublicDataPlaneAdoptionRecoveryArtifacts(wal, transaction, bytes.TrimSpace(restore)); err != nil {
		return err
	}
	wal, err = releasedomain.AdvancePublicDataPlaneAdoptionRecoveryWAL(
		wal, options.leaseOwner, options.leaseToken, phase, options.at, baselineDigest,
	)
	if err != nil {
		return err
	}
	if err := writePrivateJSON(options.walPath, wal); err != nil {
		return err
	}
	return writeJSON(stdout, wal)
}

func runWALVerify(args []string, stdout io.Writer) error {
	var options walFlags
	flags := flag.NewFlagSet("wal-verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	addWALArtifactFlags(flags, &options)
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || options.transactionPath == "" || options.restorePath == "" || options.walPath == "" {
		return fmt.Errorf("flags are invalid")
	}
	transaction, restore, _, err := readRecoveryArtifacts(options.transactionPath, options.restorePath)
	if err != nil {
		return err
	}
	var wal releasedomain.PublicDataPlaneAdoptionRecoveryWAL
	if err := readStrictJSONFile(options.walPath, &wal); err != nil {
		return err
	}
	if err := releasedomain.VerifyPublicDataPlaneAdoptionRecoveryArtifacts(wal, transaction, bytes.TrimSpace(restore)); err != nil {
		return err
	}
	return writeJSON(stdout, wal)
}

func readRecoveryArtifacts(transactionPath, restorePath string) (
	[]byte, []byte, releasedomain.PublicDataPlaneAdoptionTransactionEnvelope, error,
) {
	transaction, err := readLimitedFile(transactionPath)
	if err != nil {
		return nil, nil, releasedomain.PublicDataPlaneAdoptionTransactionEnvelope{}, err
	}
	restore, err := readLimitedFile(restorePath)
	if err != nil {
		return nil, nil, releasedomain.PublicDataPlaneAdoptionTransactionEnvelope{}, err
	}
	var envelope releasedomain.PublicDataPlaneAdoptionTransactionEnvelope
	if err := readStrictJSONFile(transactionPath, &envelope); err != nil {
		return nil, nil, releasedomain.PublicDataPlaneAdoptionTransactionEnvelope{}, err
	}
	if err := releasedomain.VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil {
		return nil, nil, releasedomain.PublicDataPlaneAdoptionTransactionEnvelope{}, err
	}
	return transaction, restore, envelope, nil
}

func loadInput(options commonFlags, snapshotName string) (releasedomain.PublicDataPlaneAdoptionInput, error) {
	prefix := ""
	if snapshotName == "prewrite-snapshot.json" {
		prefix = "prewrite-"
	}
	return loadInputSet(options, prefix)
}

func loadInputSet(options commonFlags, prefix string) (releasedomain.PublicDataPlaneAdoptionInput, error) {
	read := func(name string) ([]byte, error) { return readEvidence(options.evidenceDir, name) }
	ownership, err := read("ownership.yaml")
	if err != nil {
		return releasedomain.PublicDataPlaneAdoptionInput{}, err
	}
	values, err := read(prefix + "values.yaml")
	if err != nil {
		return releasedomain.PublicDataPlaneAdoptionInput{}, err
	}
	base, err := read(prefix + "base.yaml")
	if err != nil {
		return releasedomain.PublicDataPlaneAdoptionInput{}, err
	}
	target, err := read(prefix + "target.yaml")
	if err != nil {
		return releasedomain.PublicDataPlaneAdoptionInput{}, err
	}
	repeated, err := read(prefix + "repeated-target.yaml")
	if err != nil {
		return releasedomain.PublicDataPlaneAdoptionInput{}, err
	}
	observed, err := read(prefix + "observed.yaml")
	if err != nil {
		return releasedomain.PublicDataPlaneAdoptionInput{}, err
	}
	snapshot, err := read(prefix + "snapshot.json")
	if err != nil {
		return releasedomain.PublicDataPlaneAdoptionInput{}, err
	}
	return releasedomain.PublicDataPlaneAdoptionInput{
		Ownership: ownership, Values: values, BaseManifest: base, TargetManifest: target, RepeatedTarget: repeated,
		ObservedLive: observed, KubernetesSnapshot: snapshot, SourceCommit: options.source,
		ReleaseName: options.releaseName, ReleaseNamespace: options.namespace, ReleaseFullname: options.fullname,
		BaseRevision: options.baseRev, TargetRevision: options.targetRev, Bindings: options.bindings,
	}, nil
}

func verifyGitAncestry(ancestor, target string) error {
	for _, sha := range []string{ancestor, target} {
		command := exec.Command("git", "cat-file", "-e", sha+"^{commit}")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		if err := command.Run(); err != nil {
			return fmt.Errorf("required commit is unavailable")
		}
	}
	command := exec.Command("git", "merge-base", "--is-ancestor", ancestor, target)
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Run(); err != nil {
		return fmt.Errorf("public release record commit is not a source ancestor")
	}
	return nil
}

func readEvidence(directory, name string) ([]byte, error) {
	if filepath.Clean(directory) == "." || filepath.Base(name) != name {
		return nil, fmt.Errorf("evidence path is invalid")
	}
	return readLimitedFile(filepath.Join(directory, name))
}

func readLimitedFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxInputBytes {
		return nil, fmt.Errorf("evidence file is invalid")
	}
	return os.ReadFile(path)
}

func readStrictJSONFile(path string, target any) error {
	data, err := readLimitedFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("evidence JSON is invalid")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("evidence JSON has trailing data")
	}
	return nil
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writePrivateJSON(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writePrivateFile(path, append(encoded, '\n'))
}

func writePrivateFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".adoption-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
