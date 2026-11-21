package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"fugue/internal/releaseguardian"
)

const frontRepairReleasePolicySchema = "edge-front-repair-release-policy/v1"

type frontRepairReleasePolicy struct {
	Schema         string                  `json:"schema"`
	Scope          string                  `json:"scope"`
	Component      string                  `json:"component"`
	Repository     string                  `json:"repository"`
	RepositoryID   uint64                  `json:"repositoryId"`
	WorkflowID     uint64                  `json:"workflowId"`
	ExecutorCommit string                  `json:"executorCommit"`
	VerifierKey    string                  `json:"verifierKey"`
	APIFence       frontRepairFenceRelease `json:"apiFence"`
	GuardianFence  frontRepairFenceRelease `json:"guardianFence"`
}

func (policy frontRepairReleasePolicy) approvalTrust() (frontRepairApprovalTrust, error) {
	if policy.Schema != frontRepairReleasePolicySchema ||
		(releaseguardian.Key{Component: policy.Scope}).Validate() != nil ||
		(releaseguardian.Key{Component: policy.Component}).Validate() != nil ||
		!frontRepairGitHubRepositoryName.MatchString(policy.Repository) ||
		policy.RepositoryID == 0 || policy.WorkflowID == 0 || !exactSourceSHA(policy.ExecutorCommit) {
		return frontRepairApprovalTrust{}, errors.New("Front repair policy must independently pin scope, component, executor and workflow")
	}
	encoding := base64.RawURLEncoding.Strict()
	key, err := encoding.DecodeString(policy.VerifierKey)
	if err != nil || len(key) != ed25519.PublicKeySize || encoding.EncodeToString(key) != policy.VerifierKey {
		return frontRepairApprovalTrust{}, errors.New("Front repair policy lacks a canonical Ed25519 public key")
	}
	return frontRepairApprovalTrust{Scope: policy.Scope, ExecutorCommit: policy.ExecutorCommit, PublicKey: ed25519.PublicKey(key)}, nil
}

type frontRepairQualifiedRelease struct {
	intent   frontRepairReviewedIntent
	trust    frontRepairApprovalTrust
	artifact frontRepairVerifiedArtifact
}

// The production caller must independently supply the protected policy and
// prove it is running the policy's executor commit. The standalone preflight
// checks artifacts and a signed preview only and is not a release authority.
func qualifyFrontRepairRelease(ctx context.Context, policy frontRepairReleasePolicy, envelope frontRepairApprovalEnvelope,
	preview frontRepairPreview, get frontRepairActionsGet, now time.Time) (frontRepairQualifiedRelease, error) {
	trust, err := policy.approvalTrust()
	if err != nil {
		return frontRepairQualifiedRelease{}, err
	}
	if preview.Release.Component != policy.Component || envelope.Artifact.RepositoryID != policy.RepositoryID {
		return frontRepairQualifiedRelease{}, errors.New("Front repair signed preview or artifact is outside the selected release policy")
	}
	intent, err := envelope.authenticatePreview(preview, trust, now)
	if err != nil {
		return frontRepairQualifiedRelease{}, err
	}
	artifact, err := fetchFrontRepairArtifact(ctx, get, policy.Repository, frontRepairActionsPolicy{
		RepositoryID: policy.RepositoryID, WorkflowID: policy.WorkflowID, SourceCommit: trust.ExecutorCommit,
	}, envelope.Artifact, now)
	if err != nil {
		return frontRepairQualifiedRelease{}, err
	}
	trust.Artifact = artifact.identity
	verified, err := envelope.verify(preview, trust, artifact.tools, now)
	if err != nil || verified.AuthorizationDigest != intent.AuthorizationDigest {
		return frontRepairQualifiedRelease{}, errors.Join(errors.New("Front repair verified build does not match signed approval"), err)
	}
	return frontRepairQualifiedRelease{intent: verified, trust: trust, artifact: artifact}, nil
}

type frontRepairReleaseCheckReceipt struct {
	Schema              string                      `json:"schema"`
	Component           string                      `json:"component"`
	Group               string                      `json:"group"`
	PreviewDigest       string                      `json:"previewDigest"`
	AuthorizationDigest string                      `json:"authorizationDigest"`
	TargetGeneration    uint64                      `json:"targetGeneration"`
	MemberCount         int                         `json:"memberCount"`
	Artifact            frontRepairArtifactIdentity `json:"artifact"`
	ExecutorCommit      string                      `json:"executorCommit"`
}

func runFrontRepairReleaseCheck(args []string, stdout io.Writer) error {
	return runFrontRepairReleaseCheckWithFetcher(args, stdout, readFrontRepairGitHubCLI, time.Now().UTC())
}

func runFrontRepairReleaseCheckWithFetcher(args []string, stdout io.Writer, get frontRepairActionsGet, now time.Time) error {
	flags := flag.NewFlagSet("front-repair-release-check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	policyFile := flags.String("policy", "", "independent pinned policy JSON")
	previewFile := flags.String("preview", "", "read-only Front preview JSON")
	approvalFile := flags.String("approval", "", "signed approval JSON")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || flags.NFlag() != 3 || stdout == nil {
		return errors.New("front-repair-release-check requires policy, preview and approval files; it does not apply repairs")
	}
	var policy frontRepairReleasePolicy
	var preview frontRepairPreview
	var approval frontRepairApprovalEnvelope
	if err := readFrontRepairReleaseInput(*policyFile, "policy", 16<<10, &policy); err != nil {
		return err
	}
	if err := readFrontRepairReleaseInput(*previewFile, "preview", 512<<10, &preview); err != nil {
		return err
	}
	if err := readFrontRepairReleaseInput(*approvalFile, "approval", 16<<10, &approval); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	release, err := qualifyFrontRepairRelease(ctx, policy, approval, preview, get, now)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(frontRepairReleaseCheckReceipt{
		Schema: "edge-front-repair-release-check/v1", Component: policy.Component, Group: release.intent.Group,
		PreviewDigest: preview.Digest, AuthorizationDigest: release.intent.AuthorizationDigest,
		TargetGeneration: preview.Plan.TargetGeneration, MemberCount: len(release.intent.Nodes),
		Artifact: release.artifact.identity, ExecutorCommit: release.trust.ExecutorCommit,
	})
}

func readFrontRepairReleaseInput(path, label string, limit int64, value any) error {
	if path == "" {
		return fmt.Errorf("Front repair %s input is missing", label)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > limit {
		return fmt.Errorf("Front repair %s is not a bounded regular file", label)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot open the Front repair %s", label)
	}
	defer file.Close()
	stable, err := file.Stat()
	if err != nil || !os.SameFile(info, stable) || stable.Size() != info.Size() {
		return fmt.Errorf("Front repair %s changed before reading", label)
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(raw)) != info.Size() || decodeStrictJSON(raw, value) != nil {
		return fmt.Errorf("Front repair %s must be one complete bounded JSON document", label)
	}
	return nil
}
