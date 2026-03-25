package sourceimport

import "testing"

func TestParseGitRemoteHeadOutput(t *testing.T) {
	t.Parallel()

	branch, commitSHA, err := parseGitRemoteHeadOutput("ref: refs/heads/main HEAD\nabcdef1234567890\tHEAD\n")
	if err != nil {
		t.Fatalf("parse head output: %v", err)
	}
	if branch != "main" {
		t.Fatalf("expected branch main, got %q", branch)
	}
	if commitSHA != "abcdef1234567890" {
		t.Fatalf("expected commit abcdef1234567890, got %q", commitSHA)
	}
}

func TestParseGitRemoteBranchOutput(t *testing.T) {
	t.Parallel()

	commitSHA, err := parseGitRemoteBranchOutput("abcdef1234567890\trefs/heads/release\n", "release")
	if err != nil {
		t.Fatalf("parse branch output: %v", err)
	}
	if commitSHA != "abcdef1234567890" {
		t.Fatalf("expected commit abcdef1234567890, got %q", commitSHA)
	}
}

func TestParseGitRemoteBranchOutputRejectsMissingBranch(t *testing.T) {
	t.Parallel()

	if _, err := parseGitRemoteBranchOutput("abcdef1234567890\trefs/heads/main\n", "release"); err == nil {
		t.Fatal("expected missing branch to return an error")
	}
}
