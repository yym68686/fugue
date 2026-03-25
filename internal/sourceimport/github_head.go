package sourceimport

import (
	"context"
	"fmt"
	"strings"
)

func LatestPublicGitHubCommit(ctx context.Context, repoURL, branch string) (string, string, error) {
	if _, _, err := parseGitHubRepoURL(repoURL); err != nil {
		return "", "", err
	}

	branch = strings.TrimSpace(branch)
	if branch == "" {
		output, err := runCombinedOutput(ctx, "", "git", "ls-remote", "--symref", strings.TrimSpace(repoURL), "HEAD")
		if err != nil {
			return "", "", fmt.Errorf("git ls-remote HEAD: %w: %s", err, strings.TrimSpace(string(output)))
		}
		resolvedBranch, commitSHA, err := parseGitRemoteHeadOutput(string(output))
		if err != nil {
			return "", "", err
		}
		return commitSHA, resolvedBranch, nil
	}

	output, err := runCombinedOutput(ctx, "", "git", "ls-remote", "--heads", strings.TrimSpace(repoURL), "refs/heads/"+branch)
	if err != nil {
		return "", "", fmt.Errorf("git ls-remote branch: %w: %s", err, strings.TrimSpace(string(output)))
	}
	commitSHA, err := parseGitRemoteBranchOutput(string(output), branch)
	if err != nil {
		return "", "", err
	}
	return commitSHA, branch, nil
}

func parseGitRemoteHeadOutput(raw string) (string, string, error) {
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	var branch string
	var commitSHA string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "ref:") {
			fields := strings.Fields(line)
			if len(fields) >= 3 && strings.TrimSpace(fields[2]) == "HEAD" {
				branch = strings.TrimPrefix(strings.TrimSpace(fields[1]), "refs/heads/")
			}
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && strings.TrimSpace(fields[1]) == "HEAD" {
			commitSHA = strings.TrimSpace(fields[0])
		}
	}
	if branch == "" || commitSHA == "" {
		return "", "", fmt.Errorf("parse git ls-remote HEAD output")
	}
	return branch, commitSHA, nil
}

func parseGitRemoteBranchOutput(raw, branch string) (string, error) {
	ref := "refs/heads/" + strings.TrimSpace(branch)
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		if strings.TrimSpace(fields[1]) != ref {
			continue
		}
		commitSHA := strings.TrimSpace(fields[0])
		if commitSHA == "" {
			break
		}
		return commitSHA, nil
	}
	return "", fmt.Errorf("branch %q was not found", strings.TrimSpace(branch))
}
