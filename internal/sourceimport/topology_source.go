package sourceimport

import "strings"

func (stack GitHubComposeStack) Topology() NormalizedTopology {
	return NormalizedTopology{
		SourceKind:      TopologySourceKindCompose,
		SourcePath:      strings.TrimSpace(stack.ComposePath),
		RepoOwner:       strings.TrimSpace(stack.RepoOwner),
		RepoName:        strings.TrimSpace(stack.RepoName),
		Branch:          strings.TrimSpace(stack.Branch),
		CommitSHA:       strings.TrimSpace(stack.CommitSHA),
		DefaultAppName:  strings.TrimSpace(stack.DefaultAppName),
		Services:        append([]ComposeService(nil), stack.Services...),
		Warnings:        append([]string(nil), stack.Warnings...),
		InferenceReport: append([]TopologyInference(nil), stack.InferenceReport...),
	}
}

func (manifest GitHubFugueManifest) Topology() NormalizedTopology {
	return NormalizedTopology{
		SourceKind:        TopologySourceKindFugue,
		SourcePath:        strings.TrimSpace(manifest.ManifestPath),
		RepoOwner:         strings.TrimSpace(manifest.RepoOwner),
		RepoName:          strings.TrimSpace(manifest.RepoName),
		Branch:            strings.TrimSpace(manifest.Branch),
		CommitSHA:         strings.TrimSpace(manifest.CommitSHA),
		CommitCommittedAt: strings.TrimSpace(manifest.CommitCommittedAt),
		DefaultAppName:    strings.TrimSpace(manifest.DefaultAppName),
		PrimaryService:    strings.TrimSpace(manifest.PrimaryService),
		Services:          append([]ComposeService(nil), manifest.Services...),
		Warnings:          append([]string(nil), manifest.Warnings...),
		InferenceReport:   append([]TopologyInference(nil), manifest.InferenceReport...),
	}
}
