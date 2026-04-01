package sourceimport

import (
	"strings"

	"fugue/internal/model"
)

const (
	defaultBuilderBuildNodeLabelKey     = "fugue.io/build"
	defaultBuilderBuildNodeLabelValue   = "true"
	defaultBuilderLargeNodeLabelKey     = "fugue.io/build-tier"
	defaultBuilderLargeNodeLabelValue   = "large"
	defaultBuilderMediumNodeLabelValue  = "medium"
	defaultBuilderSmallNodeLabelValue   = "small"
	defaultBuilderCandidateCount        = 3
	defaultBuilderSelectionTimeoutSec   = 120
	defaultBuilderRetryIntervalSec      = 5
	defaultBuilderReservationLeaseSec   = 120
	defaultBuilderPreferredNodeAffinity = 100
)

type builderWorkloadProfile string

const (
	builderWorkloadProfileLight builderWorkloadProfile = "light"
	builderWorkloadProfileHeavy builderWorkloadProfile = "heavy"
)

type BuilderResourceRequirements struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

type BuilderWorkloadPolicy struct {
	Resources           BuilderResourceRequirements `json:"resources,omitempty"`
	WorkspaceSizeLimit  string                      `json:"workspaceSizeLimit,omitempty"`
	DockerDataSizeLimit string                      `json:"dockerDataSizeLimit,omitempty"`
}

type BuilderToleration struct {
	Key      string `json:"key,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
	Effect   string `json:"effect,omitempty"`
}

type BuilderPodPolicy struct {
	BuildNodeLabelKey            string                `json:"buildNodeLabelKey,omitempty"`
	BuildNodeLabelValue          string                `json:"buildNodeLabelValue,omitempty"`
	LargeNodeLabelKey            string                `json:"largeNodeLabelKey,omitempty"`
	LargeNodeLabelValue          string                `json:"largeNodeLabelValue,omitempty"`
	MediumNodeLabelValue         string                `json:"mediumNodeLabelValue,omitempty"`
	SmallNodeLabelValue          string                `json:"smallNodeLabelValue,omitempty"`
	CandidateCount               int                   `json:"candidateCount,omitempty"`
	SelectionTimeoutSeconds      int                   `json:"selectionTimeoutSeconds,omitempty"`
	RetryIntervalSeconds         int                   `json:"retryIntervalSeconds,omitempty"`
	ReservationLeaseDurationSecs int                   `json:"reservationLeaseDurationSeconds,omitempty"`
	Tolerations                  []BuilderToleration   `json:"tolerations,omitempty"`
	Light                        BuilderWorkloadPolicy `json:"light,omitempty"`
	Heavy                        BuilderWorkloadPolicy `json:"heavy,omitempty"`
}

func defaultBuilderPodPolicy() BuilderPodPolicy {
	return BuilderPodPolicy{
		BuildNodeLabelKey:            defaultBuilderBuildNodeLabelKey,
		BuildNodeLabelValue:          defaultBuilderBuildNodeLabelValue,
		LargeNodeLabelKey:            defaultBuilderLargeNodeLabelKey,
		LargeNodeLabelValue:          defaultBuilderLargeNodeLabelValue,
		MediumNodeLabelValue:         defaultBuilderMediumNodeLabelValue,
		SmallNodeLabelValue:          defaultBuilderSmallNodeLabelValue,
		CandidateCount:               defaultBuilderCandidateCount,
		SelectionTimeoutSeconds:      defaultBuilderSelectionTimeoutSec,
		RetryIntervalSeconds:         defaultBuilderRetryIntervalSec,
		ReservationLeaseDurationSecs: defaultBuilderReservationLeaseSec,
		Light: BuilderWorkloadPolicy{
			Resources: BuilderResourceRequirements{
				Requests: map[string]string{
					"cpu":               "250m",
					"memory":            "512Mi",
					"ephemeral-storage": "1Gi",
				},
				Limits: map[string]string{
					"cpu":               "1",
					"memory":            "2Gi",
					"ephemeral-storage": "3Gi",
				},
			},
			WorkspaceSizeLimit:  "2Gi",
			DockerDataSizeLimit: "4Gi",
		},
		Heavy: BuilderWorkloadPolicy{
			Resources: BuilderResourceRequirements{
				Requests: map[string]string{
					"cpu":               "750m",
					"memory":            "1Gi",
					"ephemeral-storage": "3Gi",
				},
				Limits: map[string]string{
					"cpu":               "4",
					"memory":            "6Gi",
					"ephemeral-storage": "8Gi",
				},
			},
			WorkspaceSizeLimit:  "4Gi",
			DockerDataSizeLimit: "8Gi",
		},
	}
}

func normalizeBuilderPodPolicy(policy BuilderPodPolicy) BuilderPodPolicy {
	defaults := defaultBuilderPodPolicy()
	if strings.TrimSpace(policy.BuildNodeLabelKey) == "" {
		policy.BuildNodeLabelKey = defaults.BuildNodeLabelKey
	}
	if strings.TrimSpace(policy.BuildNodeLabelValue) == "" {
		policy.BuildNodeLabelValue = defaults.BuildNodeLabelValue
	}
	if strings.TrimSpace(policy.LargeNodeLabelKey) == "" {
		policy.LargeNodeLabelKey = defaults.LargeNodeLabelKey
	}
	if strings.TrimSpace(policy.LargeNodeLabelValue) == "" {
		policy.LargeNodeLabelValue = defaults.LargeNodeLabelValue
	}
	if strings.TrimSpace(policy.MediumNodeLabelValue) == "" {
		policy.MediumNodeLabelValue = defaults.MediumNodeLabelValue
	}
	if strings.TrimSpace(policy.SmallNodeLabelValue) == "" {
		policy.SmallNodeLabelValue = defaults.SmallNodeLabelValue
	}
	if policy.CandidateCount <= 0 {
		policy.CandidateCount = defaults.CandidateCount
	}
	if policy.SelectionTimeoutSeconds <= 0 {
		policy.SelectionTimeoutSeconds = defaults.SelectionTimeoutSeconds
	}
	if policy.RetryIntervalSeconds <= 0 {
		policy.RetryIntervalSeconds = defaults.RetryIntervalSeconds
	}
	if policy.ReservationLeaseDurationSecs <= 0 {
		policy.ReservationLeaseDurationSecs = defaults.ReservationLeaseDurationSecs
	}
	policy.Tolerations = normalizeBuilderTolerations(policy.Tolerations)
	policy.Light = normalizeBuilderWorkloadPolicy(policy.Light, defaults.Light)
	policy.Heavy = normalizeBuilderWorkloadPolicy(policy.Heavy, defaults.Heavy)
	return policy
}

func normalizeBuilderTolerations(tolerations []BuilderToleration) []BuilderToleration {
	if len(tolerations) == 0 {
		return nil
	}
	normalized := make([]BuilderToleration, 0, len(tolerations))
	for _, toleration := range tolerations {
		normalizedToleration := BuilderToleration{
			Key:      strings.TrimSpace(toleration.Key),
			Operator: strings.TrimSpace(toleration.Operator),
			Value:    strings.TrimSpace(toleration.Value),
			Effect:   strings.TrimSpace(toleration.Effect),
		}
		if normalizedToleration.Key == "" &&
			normalizedToleration.Operator == "" &&
			normalizedToleration.Value == "" &&
			normalizedToleration.Effect == "" {
			continue
		}
		normalized = append(normalized, normalizedToleration)
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func normalizeBuilderWorkloadPolicy(policy, defaults BuilderWorkloadPolicy) BuilderWorkloadPolicy {
	policy.Resources = normalizeBuilderResourceRequirements(policy.Resources, defaults.Resources)
	if strings.TrimSpace(policy.WorkspaceSizeLimit) == "" {
		policy.WorkspaceSizeLimit = defaults.WorkspaceSizeLimit
	}
	if strings.TrimSpace(policy.DockerDataSizeLimit) == "" {
		policy.DockerDataSizeLimit = defaults.DockerDataSizeLimit
	}
	return policy
}

func normalizeBuilderResourceRequirements(resources, defaults BuilderResourceRequirements) BuilderResourceRequirements {
	return BuilderResourceRequirements{
		Requests: mergeBuilderStringMap(defaults.Requests, resources.Requests),
		Limits:   mergeBuilderStringMap(defaults.Limits, resources.Limits),
	}
}

func mergeBuilderStringMap(defaults, override map[string]string) map[string]string {
	if len(defaults) == 0 && len(override) == 0 {
		return nil
	}
	merged := make(map[string]string, len(defaults)+len(override))
	for key, value := range defaults {
		merged[key] = value
	}
	for key, value := range override {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			merged[key] = trimmed
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func builderWorkloadProfileFor(buildStrategy string, stateful bool) builderWorkloadProfile {
	switch strings.TrimSpace(strings.ToLower(buildStrategy)) {
	case model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks, model.AppBuildStrategyDockerfile:
		return builderWorkloadProfileHeavy
	}
	if stateful {
		return builderWorkloadProfileHeavy
	}
	return builderWorkloadProfileLight
}

func applyBuilderPodPolicy(podSpec map[string]any, policy BuilderPodPolicy, profile builderWorkloadProfile) {
	policy = normalizeBuilderPodPolicy(policy)
	workloadPolicy := policy.Light
	if profile == builderWorkloadProfileHeavy {
		workloadPolicy = policy.Heavy
	}
	applyBuilderResources(podSpec, workloadPolicy.Resources)
	applyBuilderVolumeSizeLimits(podSpec, workloadPolicy)
	applyBuilderTolerations(podSpec, policy.Tolerations)
}

func applyBuilderResources(podSpec map[string]any, resources BuilderResourceRequirements) {
	resourceObject := buildBuilderResourceObject(resources)
	if len(resourceObject) == 0 {
		return
	}
	if containers, ok := podSpec["containers"].([]map[string]any); ok {
		for index := range containers {
			containers[index]["resources"] = resourceObject
		}
	}
	if initContainers, ok := podSpec["initContainers"].([]map[string]any); ok {
		for index := range initContainers {
			initContainers[index]["resources"] = resourceObject
		}
	}
}

func buildBuilderResourceObject(resources BuilderResourceRequirements) map[string]any {
	object := map[string]any{}
	if requests := cloneBuilderStringMap(resources.Requests); len(requests) > 0 {
		object["requests"] = requests
	}
	if limits := cloneBuilderStringMap(resources.Limits); len(limits) > 0 {
		object["limits"] = limits
	}
	if len(object) == 0 {
		return nil
	}
	return object
}

func cloneBuilderStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func applyBuilderVolumeSizeLimits(podSpec map[string]any, policy BuilderWorkloadPolicy) {
	volumes, ok := podSpec["volumes"].([]map[string]any)
	if !ok {
		return
	}
	for index := range volumes {
		emptyDir, ok := volumes[index]["emptyDir"].(map[string]any)
		if !ok {
			continue
		}
		switch strings.TrimSpace(asString(volumes[index]["name"])) {
		case "workspace":
			if limit := strings.TrimSpace(policy.WorkspaceSizeLimit); limit != "" {
				emptyDir["sizeLimit"] = limit
			}
		case "docker-data":
			if limit := strings.TrimSpace(policy.DockerDataSizeLimit); limit != "" {
				emptyDir["sizeLimit"] = limit
			}
		}
	}
}

func applyBuilderPreferredLargeNodeAffinity(podSpec map[string]any, policy BuilderPodPolicy) {
	labelKey := strings.TrimSpace(policy.LargeNodeLabelKey)
	labelValue := strings.TrimSpace(policy.LargeNodeLabelValue)
	if labelKey == "" || labelValue == "" {
		return
	}
	podSpec["affinity"] = map[string]any{
		"nodeAffinity": map[string]any{
			"preferredDuringSchedulingIgnoredDuringExecution": []map[string]any{
				{
					"weight": defaultBuilderPreferredNodeAffinity,
					"preference": map[string]any{
						"matchExpressions": []map[string]any{
							{
								"key":      labelKey,
								"operator": "In",
								"values":   []string{labelValue},
							},
						},
					},
				},
			},
		},
	}
}

func applyBuilderTolerations(podSpec map[string]any, tolerations []BuilderToleration) {
	tolerations = normalizeBuilderTolerations(tolerations)
	if len(tolerations) == 0 {
		return
	}
	values := make([]map[string]any, 0, len(tolerations))
	for _, toleration := range tolerations {
		value := map[string]any{}
		if toleration.Key != "" {
			value["key"] = toleration.Key
		}
		if toleration.Operator != "" {
			value["operator"] = toleration.Operator
		}
		if toleration.Value != "" {
			value["value"] = toleration.Value
		}
		if toleration.Effect != "" {
			value["effect"] = toleration.Effect
		}
		if len(value) == 0 {
			continue
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return
	}
	podSpec["tolerations"] = values
}

func applyBuilderPlacement(podSpec map[string]any, placement builderJobPlacement) {
	if len(placement.CandidateHostnames) == 0 {
		return
	}

	requiredValues := make([]string, 0, len(placement.CandidateHostnames))
	seen := make(map[string]struct{}, len(placement.CandidateHostnames))
	for _, hostname := range placement.CandidateHostnames {
		hostname = strings.TrimSpace(hostname)
		if hostname == "" {
			continue
		}
		if _, exists := seen[hostname]; exists {
			continue
		}
		seen[hostname] = struct{}{}
		requiredValues = append(requiredValues, hostname)
	}
	if len(requiredValues) == 0 {
		return
	}

	affinity, _ := podSpec["affinity"].(map[string]any)
	if affinity == nil {
		affinity = map[string]any{}
	}

	nodeAffinity := map[string]any{
		"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
			"nodeSelectorTerms": []map[string]any{
				{
					"matchExpressions": []map[string]any{
						{
							"key":      builderHostnameLabelKey,
							"operator": "In",
							"values":   requiredValues,
						},
					},
				},
			},
		},
	}

	preferredHostname := strings.TrimSpace(placement.PreferredHostname)
	if preferredHostname == "" {
		preferredHostname = requiredValues[0]
	}
	nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"] = []map[string]any{
		{
			"weight": defaultBuilderPreferredNodeAffinity,
			"preference": map[string]any{
				"matchExpressions": []map[string]any{
					{
						"key":      builderHostnameLabelKey,
						"operator": "In",
						"values":   []string{preferredHostname},
					},
				},
			},
		},
	}

	affinity["nodeAffinity"] = nodeAffinity
	podSpec["affinity"] = affinity
}

func asString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
