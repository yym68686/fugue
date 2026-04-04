package sourceimport

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
)

const (
	TopologySourceKindCompose = "compose"
	TopologySourceKindFugue   = "fugue"

	ServiceTypeApp           = "app"
	ServiceTypePostgres      = "postgres"
	ServiceTypeRedis         = "redis"
	ServiceTypeMySQL         = "mysql"
	ServiceTypeObjectStorage = "object-storage"
	ServiceTypeCustom        = "custom"

	BindingSourceExplicit  = "explicit"
	BindingSourceDependsOn = "depends_on"
	BindingSourceEnv       = "environment"

	InferenceLevelInfo    = "info"
	InferenceLevelWarning = "warning"
)

type TopologyInference struct {
	Level    string `json:"level,omitempty"`
	Category string `json:"category,omitempty"`
	Service  string `json:"service,omitempty"`
	Message  string `json:"message,omitempty"`
}

type ServiceBinding struct {
	Service string `json:"service,omitempty"`
	Source  string `json:"source,omitempty"`
}

type ServiceHealthcheck struct {
	Test          []string `json:"test,omitempty"`
	Interval      string   `json:"interval,omitempty"`
	Timeout       string   `json:"timeout,omitempty"`
	StartPeriod   string   `json:"start_period,omitempty"`
	StartInterval string   `json:"start_interval,omitempty"`
	Retries       int      `json:"retries,omitempty"`
	Disable       bool     `json:"disable,omitempty"`
}

type NormalizedTopology struct {
	SourceKind        string              `json:"source_kind,omitempty"`
	SourcePath        string              `json:"source_path,omitempty"`
	RepoOwner         string              `json:"repo_owner,omitempty"`
	RepoName          string              `json:"repo_name,omitempty"`
	Branch            string              `json:"branch,omitempty"`
	CommitSHA         string              `json:"commit_sha,omitempty"`
	CommitCommittedAt string              `json:"commit_committed_at,omitempty"`
	DefaultAppName    string              `json:"default_app_name,omitempty"`
	PrimaryService    string              `json:"primary_service,omitempty"`
	Services          []ComposeService    `json:"services,omitempty"`
	Warnings          []string            `json:"warnings,omitempty"`
	InferenceReport   []TopologyInference `json:"inference_report,omitempty"`
}

type ManagedBackingPlan struct {
	Service      ComposeService
	OwnerService string
	Consumers    []string
}

type TopologyPlan struct {
	Topology         NormalizedTopology
	PrimaryService   string
	Deployable       []ComposeService
	BindingsBySource map[string][]ServiceBinding
	ManagedBackings  map[string]ManagedBackingPlan
	Warnings         []string
	InferenceReport  []TopologyInference
}

type TopologyDeployment struct {
	ServiceHosts           map[string]string
	ManagedPostgresByOwner map[string]model.AppPostgresSpec
}

type ServiceAdapter struct {
	ServiceType string
	Aliases     []string
	DefaultPort int
	Managed     bool
	DetectImage func(image string) bool
}

var defaultServiceAdapters = []ServiceAdapter{
	{
		ServiceType: ServiceTypePostgres,
		Aliases:     []string{ServiceTypePostgres},
		DefaultPort: 5432,
		Managed:     true,
		DetectImage: isComposePostgresService,
	},
	{
		ServiceType: ServiceTypeRedis,
		Aliases:     []string{ServiceTypeRedis},
		DefaultPort: 6379,
		DetectImage: func(image string) bool {
			return matchImageRepository(image, "redis", "library/redis")
		},
	},
	{
		ServiceType: ServiceTypeMySQL,
		Aliases:     []string{ServiceTypeMySQL},
		DefaultPort: 3306,
		DetectImage: func(image string) bool {
			return matchImageRepository(image, "mysql", "library/mysql")
		},
	},
	{
		ServiceType: ServiceTypeObjectStorage,
		Aliases:     []string{ServiceTypeObjectStorage, "objectstorage", "s3", "minio"},
		DefaultPort: 9000,
		DetectImage: func(image string) bool {
			repo := imageRepository(image)
			return repo == "minio/minio" || repo == "quay.io/minio/minio"
		},
	},
	{
		ServiceType: ServiceTypeCustom,
		Aliases:     []string{ServiceTypeCustom},
	},
	{
		ServiceType: ServiceTypeApp,
		Aliases:     []string{ServiceTypeApp},
	},
}

func normalizeServiceType(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	for _, adapter := range defaultServiceAdapters {
		if raw == adapter.ServiceType {
			return adapter.ServiceType
		}
		for _, alias := range adapter.Aliases {
			if raw == alias {
				return adapter.ServiceType
			}
		}
	}
	return ""
}

func adapterForServiceType(serviceType string) ServiceAdapter {
	serviceType = normalizeServiceType(serviceType)
	for _, adapter := range defaultServiceAdapters {
		if adapter.ServiceType == serviceType {
			return adapter
		}
	}
	return ServiceAdapter{ServiceType: ServiceTypeCustom}
}

func adapterForService(service ComposeService) ServiceAdapter {
	serviceType := service.ServiceType
	if serviceType == "" {
		serviceType = ServiceTypeApp
	}
	return adapterForServiceType(serviceType)
}

func detectServiceTypeFromImage(image string) string {
	for _, adapter := range defaultServiceAdapters {
		if adapter.DetectImage == nil {
			continue
		}
		if adapter.DetectImage(image) {
			return adapter.ServiceType
		}
	}
	return ""
}

func managedBackingService(service ComposeService) bool {
	if !service.BackingService {
		return false
	}
	return adapterForService(service).Managed
}

func defaultPortForService(service ComposeService) int {
	adapter := adapterForService(service)
	if adapter.DefaultPort > 0 {
		return adapter.DefaultPort
	}
	return 0
}

func buildNormalizedTopology(sourceKind, sourcePath string, repo clonedGitHubRepo, primaryService string, services []ComposeService, warnings []string, inferences []TopologyInference) NormalizedTopology {
	return NormalizedTopology{
		SourceKind:        strings.TrimSpace(sourceKind),
		SourcePath:        strings.TrimSpace(sourcePath),
		RepoOwner:         strings.TrimSpace(repo.RepoOwner),
		RepoName:          strings.TrimSpace(repo.RepoName),
		Branch:            strings.TrimSpace(repo.Branch),
		CommitSHA:         strings.TrimSpace(repo.CommitSHA),
		CommitCommittedAt: strings.TrimSpace(repo.CommitCommittedAt),
		DefaultAppName:    strings.TrimSpace(repo.DefaultAppName),
		PrimaryService:    strings.TrimSpace(primaryService),
		Services:          append([]ComposeService(nil), services...),
		Warnings:          append([]string(nil), warnings...),
		InferenceReport:   append([]TopologyInference(nil), inferences...),
	}
}

func appendInference(report []TopologyInference, level, category, service, format string, args ...any) []TopologyInference {
	message := strings.TrimSpace(fmt.Sprintf(format, args...))
	if message == "" {
		return report
	}
	return append(report, TopologyInference{
		Level:    strings.TrimSpace(level),
		Category: strings.TrimSpace(category),
		Service:  strings.TrimSpace(service),
		Message:  message,
	})
}

func slugifyOptional(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return model.Slugify(raw)
}

func uniqueBindings(bindings []ServiceBinding) []ServiceBinding {
	if len(bindings) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(bindings))
	out := make([]ServiceBinding, 0, len(bindings))
	for _, binding := range bindings {
		service := slugifyOptional(binding.Service)
		if service == "" {
			continue
		}
		binding.Service = service
		binding.Source = strings.TrimSpace(binding.Source)
		key := binding.Service + "\x00" + binding.Source
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, binding)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service == out[j].Service {
			return out[i].Source < out[j].Source
		}
		return out[i].Service < out[j].Service
	})
	return out
}

func cloneStringMapLocal(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func imageRepository(image string) string {
	image = strings.ToLower(strings.TrimSpace(image))
	if image == "" {
		return ""
	}
	if withoutDigest, _, found := strings.Cut(image, "@"); found {
		image = withoutDigest
	}
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon > lastSlash {
		image = image[:lastColon]
	}
	switch {
	case strings.HasPrefix(image, "docker.io/"):
		image = strings.TrimPrefix(image, "docker.io/")
	case strings.HasPrefix(image, "index.docker.io/"):
		image = strings.TrimPrefix(image, "index.docker.io/")
	case strings.HasPrefix(image, "registry-1.docker.io/"):
		image = strings.TrimPrefix(image, "registry-1.docker.io/")
	}
	return image
}

func matchImageRepository(image string, repos ...string) bool {
	repo := imageRepository(image)
	for _, candidate := range repos {
		candidate = strings.ToLower(strings.TrimSpace(candidate))
		if candidate != "" && repo == candidate {
			return true
		}
	}
	return false
}
