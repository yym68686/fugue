package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func resolveGitHubToken(ctx context.Context, explicit string) (string, error) {
	if token := strings.TrimSpace(explicit); token != "" {
		return token, nil
	}
	for _, key := range []string{"FUGUE_GITHUB_TOKEN", "GITHUB_TOKEN"} {
		if token := strings.TrimSpace(os.Getenv(key)); token != "" {
			return token, nil
		}
	}
	if gh, err := exec.LookPath("gh"); err == nil {
		if out, err := exec.CommandContext(ctx, gh, "auth", "token").Output(); err == nil && strings.TrimSpace(string(out)) != "" {
			return strings.TrimSpace(string(out)), nil
		}
	}
	return "", fmt.Errorf("private GitHub repository needs credentials; set FUGUE_GITHUB_TOKEN or run gh auth login")
}
