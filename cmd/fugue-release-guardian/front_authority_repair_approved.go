package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"fugue/internal/declarativerelease"
	"k8s.io/client-go/kubernetes"
)

const frontRepairActionsApplyJob = "edge_front_repair"
const frontRepairProtectedPolicyEnv = "FUGUE_EDGE_FRONT_REPAIR_PROTECTED_POLICY"

type frontRepairActionsLaunch struct {
	policy      frontRepairReleasePolicy
	inputs      string
	declaration string
}

func frontRepairActionsLaunchContext(getenv func(string) string, build *debug.BuildInfo) (frontRepairActionsLaunch, error) {
	declaration := getenv("GITHUB_SHA")
	repo := getenv("GITHUB_REPOSITORY")
	repositoryID, idErr := strconv.ParseUint(getenv("GITHUB_REPOSITORY_ID"), 10, 64)
	if getenv("GITHUB_ACTIONS") != "true" || getenv("GITHUB_EVENT_NAME") != "push" ||
		getenv("GITHUB_REF") != "refs/heads/main" || getenv("GITHUB_REF_PROTECTED") != "true" ||
		getenv("GITHUB_JOB") != frontRepairActionsApplyJob || getenv("RUNNER_ENVIRONMENT") != "self-hosted" ||
		getenv("GITHUB_SERVER_URL") != "https://github.com" ||
		!exactSourceSHA(declaration) || !frontRepairGitHubRepositoryName.MatchString(repo) || idErr != nil || repositoryID == 0 ||
		getenv("GITHUB_WORKFLOW_REF") != repo+"/"+frontRepairActionsWorkflowPath+"@refs/heads/main" ||
		!exactSourceSHA(getenv("GITHUB_WORKFLOW_SHA")) {
		return frontRepairActionsLaunch{}, errors.New("Front repair requires its protected main push Actions job")
	}
	policyJSON := getenv(frontRepairProtectedPolicyEnv)
	var policy frontRepairReleasePolicy
	if len(policyJSON) == 0 || len(policyJSON) > 16<<10 || decodeStrictJSON([]byte(policyJSON), &policy) != nil ||
		policy.Repository != repo || policy.RepositoryID != repositoryID || policy.ExecutorCommit == declaration {
		return frontRepairActionsLaunch{}, errors.New("Front repair requires the independent protected release policy and a prior executor build")
	}
	if _, err := policy.approvalTrust(); err != nil {
		return frontRepairActionsLaunch{}, err
	}
	if err := policy.requireFenceRevisions(); err != nil {
		return frontRepairActionsLaunch{}, err
	}
	if !frontRepairExecutorBuildMatches(build, policy.ExecutorCommit) {
		return frontRepairActionsLaunch{}, errors.New("Front repair launch does not use the exact clean approved Guardian build")
	}
	inputDir := getenv("FUGUE_EDGE_FRONT_REPAIR_INPUTS")
	runnerTemp := getenv("RUNNER_TEMP")
	if !filepath.IsAbs(runnerTemp) || filepath.Clean(runnerTemp) != runnerTemp ||
		!filepath.IsAbs(inputDir) || filepath.Clean(inputDir) != inputDir {
		return frontRepairActionsLaunch{}, errors.New("Front repair inputs must live in private runner scratch")
	}
	if !filepath.IsLocal(filepath.Base(inputDir)) || filepath.Dir(inputDir) != runnerTemp {
		return frontRepairActionsLaunch{}, errors.New("Front repair inputs are outside private runner scratch")
	}
	info, err := os.Lstat(inputDir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return frontRepairActionsLaunch{}, errors.New("Front repair inputs must use a plain 0700 directory")
	}
	return frontRepairActionsLaunch{policy: policy, inputs: inputDir, declaration: declaration}, nil
}

func frontRepairExecutorBuildMatches(info *debug.BuildInfo, source string) bool {
	if info == nil || info.Path != "fugue/cmd/fugue-release-guardian" || info.Main.Path != "fugue" ||
		!exactSourceSHA(source) {
		return false
	}
	want := map[string]string{"GOOS": "linux", "-buildmode": "exe", "vcs": "git",
		"vcs.revision": source, "vcs.modified": "false"}
	seen := make(map[string]bool, len(want))
	for _, setting := range info.Settings {
		if value, ok := want[setting.Key]; ok {
			if seen[setting.Key] || setting.Value != value {
				return false
			}
			seen[setting.Key] = true
		}
	}
	return len(seen) == len(want)
}

func requireFrontRepairCurrentDeclaration(ctx context.Context, launch frontRepairActionsLaunch, get frontRepairActionsGet) error {
	var branch struct {
		Name      string `json:"name"`
		Protected bool   `json:"protected"`
		Commit    struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	raw, err := get(ctx, "repos/"+launch.policy.Repository+"/branches/main", frontRepairActionsMetadataMax)
	if err != nil || json.Unmarshal(raw, &branch) != nil || branch.Name != "main" || !branch.Protected || branch.Commit.SHA != launch.declaration {
		return errors.New("Front repair declaration is no longer the protected main tip")
	}
	return nil
}

// requireFrontRepairEnvironment checks the actual GitHub configuration,
// not merely a workflow environment name or the presence of its secrets.
func requireFrontRepairEnvironment(ctx context.Context, launch frontRepairActionsLaunch, get frontRepairActionsGet) error {
	const environment = "production-edge-front-repair"
	endpoint := "repos/" + launch.policy.Repository + "/environments/" + environment
	var live struct {
		Name                   string `json:"name"`
		CanAdminsBypass        *bool  `json:"can_admins_bypass"`
		DeploymentBranchPolicy struct {
			ProtectedBranches    bool `json:"protected_branches"`
			CustomBranchPolicies bool `json:"custom_branch_policies"`
		} `json:"deployment_branch_policy"`
		Rules []struct {
			Type              string `json:"type"`
			PreventSelfReview bool   `json:"prevent_self_review"`
			Reviewers         []struct {
				Type     string `json:"type"`
				Reviewer struct {
					ID int64 `json:"id"`
				} `json:"reviewer"`
			} `json:"reviewers"`
		} `json:"protection_rules"`
	}
	raw, err := get(ctx, endpoint, frontRepairActionsMetadataMax)
	if err != nil || json.Unmarshal(raw, &live) != nil || live.Name != environment || live.CanAdminsBypass == nil ||
		*live.CanAdminsBypass || live.DeploymentBranchPolicy.ProtectedBranches || !live.DeploymentBranchPolicy.CustomBranchPolicies {
		return errors.New("Front repair requires its dedicated non-bypassable production environment")
	}
	reviewerRules := 0
	for _, rule := range live.Rules {
		if rule.Type != "required_reviewers" {
			continue
		}
		reviewerRules++
		if !rule.PreventSelfReview || len(rule.Reviewers) == 0 {
			return errors.New("Front repair requires independent deployment review")
		}
		for _, reviewer := range rule.Reviewers {
			if reviewer.Reviewer.ID <= 0 || (reviewer.Type != "User" && reviewer.Type != "Team") {
				return errors.New("Front repair environment has an invalid deployment reviewer")
			}
		}
	}
	if reviewerRules != 1 {
		return errors.New("Front repair environment has no unambiguous reviewer rule")
	}
	var branches struct {
		TotalCount int `json:"total_count"`
		Policies   []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"branch_policies"`
	}
	raw, err = get(ctx, endpoint+"/deployment-branch-policies", frontRepairActionsMetadataMax)
	if err != nil || json.Unmarshal(raw, &branches) != nil || branches.TotalCount != 1 || len(branches.Policies) != 1 ||
		branches.Policies[0].Name != "main" || branches.Policies[0].Type != "branch" {
		return errors.New("Front repair production environment must allow exactly the main branch")
	}
	return nil
}

type frontRepairApprovedReceipt struct {
	Schema              string `json:"schema"`
	DeclarationCommit   string `json:"declarationCommit"`
	AuthorizationDigest string `json:"authorizationDigest"`
	PreviewDigest       string `json:"previewDigest"`
	JournalDigest       string `json:"journalDigest"`
	Group               string `json:"group"`
	TargetGeneration    uint64 `json:"targetGeneration"`
	Completed           bool   `json:"completed"`
}

func runFrontRepairApproved(args []string, stdout io.Writer) error {
	if len(args) != 0 || stdout == nil {
		return errors.New("Front repair launcher accepts no interactive flags")
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("Guardian build identity is unavailable")
	}
	launch, err := frontRepairActionsLaunchContext(os.Getenv, info)
	if err != nil {
		return err
	}
	var preview frontRepairPreview
	var envelope frontRepairApprovalEnvelope
	if err := readFrontRepairReleaseInput(filepath.Join(launch.inputs, "preview.json"), "preview", 512<<10, &preview); err != nil {
		return err
	}
	if err := readFrontRepairReleaseInput(filepath.Join(launch.inputs, "approval.json"), "approval", 16<<10, &envelope); err != nil {
		return err
	}
	lifetime, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(lifetime, 3*time.Minute)
	defer cancel()
	qualification, err := qualifyFrontRepairRelease(ctx, launch.policy, envelope, preview, readFrontRepairGitHubCLI, time.Now().UTC())
	if err != nil {
		return err
	}
	registryFile, err := os.Open(defaultFrontRepairRegistry)
	if err != nil {
		return err
	}
	registry, err := declarativerelease.DecodeEdgeGroupRegistry(registryFile)
	closeErr := registryFile.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	controlRegistry, err := loadFrontRepairControlRegistry()
	if err != nil {
		return err
	}
	selection, err := selectFrontRepairWorker(registry, launch.policy.Component)
	if err != nil {
		return err
	}
	if err := requireFrontRepairCurrentDeclaration(ctx, launch, readFrontRepairGitHubCLI); err != nil {
		return err
	}
	if err := requireFrontRepairEnvironment(ctx, launch, readFrontRepairGitHubCLI); err != nil {
		return err
	}
	config, err := loadKubeConfig()
	if err != nil {
		return err
	}
	config.QPS, config.Burst = guardianKubeClientQPS, guardianKubeClientBurst
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	activator, err := newFrontAuthorityActivator(client, &kubePodExecutor{config: config, client: client}, selection.runtime, frontRepairActionsApplyJob)
	if err != nil {
		return err
	}
	runtime := newFrontRepairRuntime(activator)
	runtime.requireControlPlaneFences = func(ctx context.Context) error {
		return requireLiveFrontRepairFences(ctx, client, controlRegistry, launch.policy)
	}
	result, err := runtime.runApproved(ctx, selection, preview, envelope, qualification.trust, qualification.artifact)
	// A failed invocation never emits a success receipt. The immutable
	// journal resolves partial writes and unknown acknowledgements.
	if err != nil {
		return err
	}
	if !result.completed || !exactSHA256Digest(result.journal.Digest) || !qualification.intent.matches(result.journal) {
		return errors.New("Front repair did not prove exact journal completion")
	}
	return json.NewEncoder(stdout).Encode(frontRepairApprovedReceipt{
		Schema: "edge-front-repair-approved-receipt/v1", DeclarationCommit: launch.declaration,
		PreviewDigest: preview.Digest, AuthorizationDigest: qualification.intent.AuthorizationDigest, JournalDigest: result.journal.Digest,
		Group: qualification.intent.Group, TargetGeneration: preview.Plan.TargetGeneration, Completed: true,
	})
}
