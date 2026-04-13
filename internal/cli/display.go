package cli

import (
	"strings"

	"fugue/internal/model"
)

func formatDisplayName(name, id string, showIDs bool) string {
	name = strings.TrimSpace(name)
	id = strings.TrimSpace(id)
	switch {
	case name == "" && id == "":
		return ""
	case name == "":
		return id
	case !showIDs || id == "" || strings.EqualFold(name, id):
		return name
	default:
		return name + " (" + id + ")"
	}
}

func mapProjectNamesByID(projects []model.Project) map[string]string {
	out := make(map[string]string, len(projects))
	for _, project := range projects {
		if strings.TrimSpace(project.ID) == "" {
			continue
		}
		out[project.ID] = project.Name
	}
	return out
}

func mapRuntimeNames(runtimes []model.Runtime) map[string]string {
	out := make(map[string]string, len(runtimes))
	for _, runtime := range runtimes {
		if strings.TrimSpace(runtime.ID) == "" {
			continue
		}
		out[runtime.ID] = runtime.Name
	}
	return out
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
