package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"time"
)

var frontRepairGitHubRepositoryName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9_][A-Za-z0-9_.-]{0,99}$`)

type frontRepairActionsGet func(context.Context, string, int64) ([]byte, error)

// fetchFrontRepairArtifact uses only exact, read-only GitHub endpoints.
// The release launcher must take the repository and policy from protected
// configuration independently of the approval. No cluster client is involved.
func fetchFrontRepairArtifact(ctx context.Context, get frontRepairActionsGet, repo string, policy frontRepairActionsPolicy,
	identity frontRepairArtifactIdentity, now time.Time) (frontRepairVerifiedArtifact, error) {
	if get == nil || !frontRepairGitHubRepositoryName.MatchString(repo) || !identity.valid() ||
		identity.RepositoryID != policy.RepositoryID || policy.WorkflowID == 0 || !exactSourceSHA(policy.SourceCommit) {
		return frontRepairVerifiedArtifact{}, errors.New("Front repair artifact check requires a pinned repository, workflow, source and upload identity")
	}
	prefix := "repos/" + repo + "/actions/"
	read := func(endpoint string) (*bytes.Reader, error) {
		data, err := get(ctx, prefix+endpoint, frontRepairActionsMetadataMax)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(data), nil
	}
	run, err := read(fmt.Sprintf("runs/%d/attempts/%d", identity.WorkflowRunID, identity.RunAttempt))
	if err != nil {
		return frontRepairVerifiedArtifact{}, fmt.Errorf("fetch exact Actions attempt: %w", err)
	}
	job, err := read(fmt.Sprintf("jobs/%d", identity.ProducerJobID))
	if err != nil {
		return frontRepairVerifiedArtifact{}, fmt.Errorf("fetch exact Actions producer job: %w", err)
	}
	meta, err := read(fmt.Sprintf("artifacts/%d", identity.ArtifactID))
	if err != nil {
		return frontRepairVerifiedArtifact{}, fmt.Errorf("fetch exact Actions artifact metadata: %w", err)
	}
	record, err := readFrontRepairActionsMetadata(policy, identity, run, job, meta, now)
	if err != nil {
		return frontRepairVerifiedArtifact{}, err
	}
	raw, err := get(ctx, prefix+fmt.Sprintf("artifacts/%d/zip", identity.ArtifactID), record.Size)
	if err != nil {
		return frontRepairVerifiedArtifact{}, fmt.Errorf("fetch exact Actions archive: %w", err)
	}
	return readFrontRepairActionsArchive(policy, identity, record, bytes.NewReader(raw))
}

func readFrontRepairGitHubCLI(ctx context.Context, endpoint string, limit int64) ([]byte, error) {
	if limit <= 0 || limit > frontRepairActionsArchiveMax {
		return nil, errors.New("invalid Actions response byte limit")
	}
	command := exec.CommandContext(ctx, "gh", "api", "--hostname", "github.com", "--method", "GET", endpoint)
	command.Env = append(os.Environ(), "GH_PROMPT_DISABLED=1", "GH_DEBUG=")
	command.Stderr = io.Discard
	output := &frontRepairBoundedOutput{remaining: limit}
	command.Stdout = output
	if err := command.Run(); err != nil || output.exceeded {
		// Never emit redirect locations, token context or remote response text.
		return nil, errors.New("authenticated GitHub read failed or exceeded its byte limit")
	}
	return output.data.Bytes(), nil
}

type frontRepairBoundedOutput struct {
	data      bytes.Buffer
	remaining int64
	exceeded  bool
}

func (w *frontRepairBoundedOutput) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		w.exceeded = true
		// Return an error rather than draining a response of arbitrary size.
		return 0, errors.New("Actions response exceeds its byte limit")
	}
	n, err := w.data.Write(p)
	w.remaining -= int64(n)
	return n, err
}

type frontRepairArtifactCheckReceipt struct {
	Artifact           frontRepairArtifactIdentity `json:"artifact"`
	SourceCommit       string                      `json:"sourceCommit"`
	ToolManifestDigest string                      `json:"toolManifestDigest"`
}

func runFrontRepairArtifactCheck(args []string, stdout io.Writer) error {
	return runFrontRepairArtifactCheckWithFetcher(args, stdout, readFrontRepairGitHubCLI, time.Now().UTC())
}

func runFrontRepairArtifactCheckWithFetcher(args []string, stdout io.Writer, get frontRepairActionsGet, now time.Time) error {
	flags := flag.NewFlagSet("front-repair-artifact-check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repo := flags.String("repository", "", "exact owner/repo")
	var policy frontRepairActionsPolicy
	var identity frontRepairArtifactIdentity
	flags.Uint64Var(&policy.RepositoryID, "repository-id", 0, "pinned repository ID")
	flags.Uint64Var(&policy.WorkflowID, "workflow-id", 0, "pinned CI workflow ID")
	flags.StringVar(&policy.SourceCommit, "source-commit", "", "pinned main source revision")
	flags.Uint64Var(&identity.WorkflowRunID, "run-id", 0, "exact producer run ID")
	attempt := flags.Uint64("run-attempt", 0, "exact producer attempt")
	flags.Uint64Var(&identity.ProducerJobID, "producer-job-id", 0, "exact prepush job ID")
	flags.Uint64Var(&identity.ArtifactID, "artifact-id", 0, "exact immutable artifact ID")
	flags.StringVar(&identity.ArchiveDigest, "archive-digest", "", "exact archive SHA-256")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || flags.NFlag() != 9 || *attempt > uint64(^uint32(0)) || stdout == nil {
		return errors.New("front-repair-artifact-check requires all nine exact artifact and repository flags; it does not apply repairs")
	}
	identity.RepositoryID, identity.RunAttempt = policy.RepositoryID, uint32(*attempt)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	artifact, err := fetchFrontRepairArtifact(ctx, get, *repo, policy, identity, now)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(frontRepairArtifactCheckReceipt{Artifact: artifact.identity,
		SourceCommit: artifact.tools.source, ToolManifestDigest: artifact.tools.digest})
}
