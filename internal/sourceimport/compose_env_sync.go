package sourceimport

import (
	"context"
	"errors"
	"strings"

	"fugue/internal/model"
)

var ErrSourceTopologyNotFound = errors.New("source topology file not found")

type GitHubComposeServiceEnvRequest struct {
	RepoURL                string
	RepoAuthToken          string
	Branch                 string
	ComposeService         string
	AppHosts               map[string]string
	ManagedPostgresByOwner map[string]model.AppPostgresSpec
}

type UploadComposeServiceEnvRequest struct {
	ArchiveFilename        string
	ArchiveSHA256          string
	ArchiveSizeBytes       int64
	ArchiveData            []byte
	AppName                string
	ComposeService         string
	AppHosts               map[string]string
	ManagedPostgresByOwner map[string]model.AppPostgresSpec
}

func (i *Importer) SuggestGitHubComposeServiceEnv(ctx context.Context, req GitHubComposeServiceEnvRequest) (map[string]string, error) {
	if strings.TrimSpace(req.ComposeService) == "" {
		return nil, nil
	}
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-compose-env-*")
	if err != nil {
		return nil, err
	}
	defer releaseClonedRepo(repo)

	return suggestComposeServiceEnvFromRepo(repo, req.ComposeService, req.AppHosts, req.ManagedPostgresByOwner)
}

func (i *Importer) SuggestUploadedComposeServiceEnv(_ context.Context, req UploadComposeServiceEnvRequest) (map[string]string, error) {
	if strings.TrimSpace(req.ComposeService) == "" || len(req.ArchiveData) == 0 {
		return nil, nil
	}
	src, err := i.extractUploadedArchive(UploadSourceImportRequest{
		UploadID:         "compose-env-sync",
		ArchiveFilename:  req.ArchiveFilename,
		ArchiveSHA256:    req.ArchiveSHA256,
		ArchiveSizeBytes: req.ArchiveSizeBytes,
		ArchiveData:      req.ArchiveData,
		AppName:          req.AppName,
	})
	if err != nil {
		return nil, err
	}
	defer releaseExtractedUploadSource(src)

	return suggestComposeServiceEnvFromRepo(clonedGitHubRepo{
		RepoDir:        src.RootDir,
		DefaultAppName: src.DefaultAppName,
	}, req.ComposeService, req.AppHosts, req.ManagedPostgresByOwner)
}

func suggestComposeServiceEnvFromRepo(repo clonedGitHubRepo, composeService string, appHosts map[string]string, managedPostgresByOwner map[string]model.AppPostgresSpec) (map[string]string, error) {
	topology, err := inspectImportableTopologyFromRepo(repo)
	if err != nil {
		return nil, err
	}
	return suggestComposeServiceEnvForTopology(topology, composeService, appHosts, managedPostgresByOwner)
}

func inspectImportableTopologyFromRepo(repo clonedGitHubRepo) (NormalizedTopology, error) {
	manifest, err := inspectFugueManifestFromRepo(repo)
	switch {
	case err == nil:
		return manifest.Topology(), nil
	case err != nil && !errors.Is(err, ErrFugueManifestNotFound):
		return NormalizedTopology{}, err
	}

	stack, err := inspectComposeStackFromRepo(repo)
	switch {
	case err == nil:
		return stack.Topology(), nil
	case err != nil && !errors.Is(err, ErrComposeNotFound):
		return NormalizedTopology{}, err
	default:
		return NormalizedTopology{}, ErrSourceTopologyNotFound
	}
}

func inspectImportableServicesFromRepo(repo clonedGitHubRepo) ([]ComposeService, error) {
	topology, err := inspectImportableTopologyFromRepo(repo)
	if err != nil {
		return nil, err
	}
	return topology.Services, nil
}

func suggestComposeServiceEnv(services []ComposeService, composeService string, appHosts map[string]string, managedPostgresByOwner map[string]model.AppPostgresSpec) (map[string]string, error) {
	return suggestComposeServiceEnvForTopology(NormalizedTopology{Services: append([]ComposeService(nil), services...)}, composeService, appHosts, managedPostgresByOwner)
}

func suggestComposeServiceEnvForTopology(topology NormalizedTopology, composeService string, appHosts map[string]string, managedPostgresByOwner map[string]model.AppPostgresSpec) (map[string]string, error) {
	composeService = slugifyOptional(composeService)
	if composeService == "" {
		return nil, nil
	}
	plan, err := AnalyzeNormalizedTopology(topology, "")
	if err != nil {
		return nil, err
	}
	env, _, err := ResolveTopologyServiceEnvironment(plan, composeService, TopologyDeployment{
		ServiceHosts:           cloneStringMapLocal(appHosts),
		ManagedPostgresByOwner: cloneManagedPostgresMap(managedPostgresByOwner),
	})
	if err != nil {
		return nil, err
	}
	if len(env) == 0 {
		return nil, nil
	}
	return env, nil
}

func cloneManagedPostgresMap(values map[string]model.AppPostgresSpec) map[string]model.AppPostgresSpec {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]model.AppPostgresSpec, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
