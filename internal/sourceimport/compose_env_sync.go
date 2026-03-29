package sourceimport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"fugue/internal/model"
)

var ErrSourceTopologyNotFound = errors.New("source topology file not found")

type GitHubComposeServiceEnvRequest struct {
	RepoURL                string
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

func (i *Importer) SuggestPublicGitHubComposeServiceEnv(ctx context.Context, req GitHubComposeServiceEnvRequest) (map[string]string, error) {
	if strings.TrimSpace(req.ComposeService) == "" {
		return nil, nil
	}
	repo, err := i.clonePublicGitHubRepo(ctx, req.RepoURL, req.Branch, "github-compose-env-*")
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
	services, err := inspectImportableServicesFromRepo(repo)
	if err != nil {
		return nil, err
	}
	return suggestComposeServiceEnv(services, composeService, appHosts, managedPostgresByOwner)
}

func inspectImportableServicesFromRepo(repo clonedGitHubRepo) ([]ComposeService, error) {
	manifest, err := inspectFugueManifestFromRepo(repo)
	switch {
	case err == nil:
		return manifest.Services, nil
	case err != nil && !errors.Is(err, ErrFugueManifestNotFound):
		return nil, err
	}

	stack, err := inspectComposeStackFromRepo(repo)
	switch {
	case err == nil:
		return stack.Services, nil
	case err != nil && !errors.Is(err, ErrComposeNotFound):
		return nil, err
	default:
		return nil, ErrSourceTopologyNotFound
	}
}

func suggestComposeServiceEnv(services []ComposeService, composeService string, appHosts map[string]string, managedPostgresByOwner map[string]model.AppPostgresSpec) (map[string]string, error) {
	composeService = model.Slugify(composeService)
	if composeService == "" {
		return nil, nil
	}

	appServices := make([]ComposeService, 0, len(services))
	postgresServices := make([]ComposeService, 0)
	var target *ComposeService
	for i := range services {
		service := services[i]
		switch service.Kind {
		case ComposeServiceKindApp:
			appServices = append(appServices, service)
		case ComposeServiceKindPostgres:
			postgresServices = append(postgresServices, service)
		}
		if service.Name == composeService {
			target = &services[i]
		}
	}
	if target == nil {
		return nil, fmt.Errorf("compose service %q not found in source topology", composeService)
	}

	serviceHosts := cloneComposeServiceMap(appHosts)
	if serviceHosts == nil {
		serviceHosts = map[string]string{}
	}
	for _, postgres := range postgresServices {
		consumers := composePostgresConsumersForSync(appServices, postgres.Name)
		if len(consumers) == 0 {
			continue
		}
		owner := pickComposePostgresOwnerForSync(consumers)
		spec, ok := managedPostgresByOwner[owner.Name]
		if !ok || strings.TrimSpace(spec.ServiceName) == "" {
			continue
		}
		serviceHosts[postgres.Name] = strings.TrimSpace(spec.ServiceName)
	}

	env := rewriteComposeEnvironmentForSync(target.Environment, serviceHosts)
	if spec, ok := managedPostgresByOwner[target.Name]; ok {
		env = applyManagedPostgresEnvironmentForSync(env, spec)
	}
	if len(env) == 0 {
		return nil, nil
	}
	return env, nil
}

func composePostgresConsumersForSync(appServices []ComposeService, postgresService string) []ComposeService {
	consumers := make([]ComposeService, 0)
	for _, service := range appServices {
		if composeServiceDependsOnForSync(service, postgresService) || composeEnvironmentReferencesServiceForSync(service.Environment, postgresService) {
			consumers = append(consumers, service)
		}
	}
	return consumers
}

func pickComposePostgresOwnerForSync(consumers []ComposeService) ComposeService {
	best := consumers[0]
	bestScore := composePostgresOwnerScoreForSync(best)
	for _, service := range consumers[1:] {
		score := composePostgresOwnerScoreForSync(service)
		if score > bestScore || (score == bestScore && service.Name < best.Name) {
			best = service
			bestScore = score
		}
	}
	return best
}

func composePostgresOwnerScoreForSync(service ComposeService) int {
	score := 0
	switch service.Name {
	case "api":
		score += 100
	case "backend":
		score += 90
	case "server":
		score += 80
	case "app":
		score += 60
	}
	if strings.Contains(service.Name, "api") || strings.Contains(service.Name, "back") {
		score += 30
	}
	return score
}

func composeServiceDependsOnForSync(service ComposeService, target string) bool {
	for _, dep := range service.DependsOn {
		if dep == target {
			return true
		}
	}
	return false
}

func composeEnvironmentReferencesServiceForSync(env map[string]string, service string) bool {
	for _, value := range env {
		if composeEnvValueReferencesServiceForSync(value, service) {
			return true
		}
	}
	return false
}

func rewriteComposeEnvironmentForSync(env map[string]string, hosts map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := make(map[string]string, len(env))
	for key, value := range env {
		out[key] = rewriteComposeEnvValueForSync(value, hosts)
	}
	return out
}

func applyManagedPostgresEnvironmentForSync(env map[string]string, spec model.AppPostgresSpec) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := cloneComposeServiceMap(env)
	overrideManagedPostgresEnvIfPresentForSync(out, "DB_HOST", spec.ServiceName)
	overrideManagedPostgresEnvIfPresentForSync(out, "POSTGRES_HOST", spec.ServiceName)
	overrideManagedPostgresEnvIfPresentForSync(out, "DB_PORT", "5432")
	overrideManagedPostgresEnvIfPresentForSync(out, "POSTGRES_PORT", "5432")
	overrideManagedPostgresEnvIfPresentForSync(out, "DB_NAME", spec.Database)
	overrideManagedPostgresEnvIfPresentForSync(out, "POSTGRES_DB", spec.Database)
	overrideManagedPostgresEnvIfPresentForSync(out, "POSTGRES_DATABASE", spec.Database)
	overrideManagedPostgresEnvIfPresentForSync(out, "DB_USER", spec.User)
	overrideManagedPostgresEnvIfPresentForSync(out, "POSTGRES_USER", spec.User)
	overrideManagedPostgresEnvIfPresentForSync(out, "DB_PASSWORD", spec.Password)
	overrideManagedPostgresEnvIfPresentForSync(out, "POSTGRES_PASSWORD", spec.Password)

	for key, value := range out {
		if rewritten, ok := rewriteManagedPostgresURLForSync(value, spec); ok {
			out[key] = rewritten
		}
	}
	return out
}

func overrideManagedPostgresEnvIfPresentForSync(env map[string]string, key, value string) {
	if _, ok := env[key]; ok {
		env[key] = value
	}
}

func rewriteManagedPostgresURLForSync(value string, spec model.AppPostgresSpec) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return value, false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return value, false
	}
	if !strings.Contains(strings.ToLower(parsed.Scheme), "postgres") {
		return value, false
	}
	if !strings.EqualFold(parsed.Hostname(), strings.TrimSpace(spec.ServiceName)) {
		return value, false
	}

	port := parsed.Port()
	if port == "" {
		port = "5432"
	}
	parsed.Host = net.JoinHostPort(spec.ServiceName, port)
	parsed.User = url.UserPassword(spec.User, spec.Password)
	if db := strings.TrimSpace(spec.Database); db != "" {
		parsed.Path = "/" + strings.TrimPrefix(db, "/")
	}
	return parsed.String(), true
}

func rewriteComposeEnvValueForSync(value string, hosts map[string]string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	if replacement, ok := hosts[value]; ok {
		return replacement
	}

	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		host := parsed.Hostname()
		if replacement, ok := hosts[host]; ok {
			if port := parsed.Port(); port != "" {
				parsed.Host = net.JoinHostPort(replacement, port)
			} else {
				parsed.Host = replacement
			}
			return parsed.String()
		}
	}

	if host, port, err := net.SplitHostPort(value); err == nil {
		if replacement, ok := hosts[host]; ok {
			return net.JoinHostPort(replacement, port)
		}
	}

	for service, replacement := range hosts {
		value = strings.ReplaceAll(value, "://"+service+":", "://"+replacement+":")
		value = strings.ReplaceAll(value, "://"+service+"/", "://"+replacement+"/")
		value = strings.ReplaceAll(value, "@"+service+":", "@"+replacement+":")
		value = strings.ReplaceAll(value, "@"+service+"/", "@"+replacement+"/")
	}
	return value
}

func composeEnvValueReferencesServiceForSync(value, service string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if value == service {
		return true
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Hostname() == service {
		return true
	}
	if host, _, err := net.SplitHostPort(value); err == nil && host == service {
		return true
	}
	return strings.Contains(value, "://"+service+":") ||
		strings.Contains(value, "://"+service+"/") ||
		strings.Contains(value, "@"+service+":") ||
		strings.Contains(value, "@"+service+"/")
}

func cloneComposeServiceMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
