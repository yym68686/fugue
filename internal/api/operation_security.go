package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

var errOperationNotVisible = errors.New("operation is not visible to this principal")

var (
	operationSecretAssignmentPattern = regexp.MustCompile(`(?i)(\b[A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|PWD|API_KEY|DATABASE_URL|DB_URL|DSN)[A-Z0-9_]*\s*[=:]\s*)([^\s&]+)`)
	operationBearerPattern           = regexp.MustCompile(`(?i)(Bearer\s+)([A-Za-z0-9._~+\-/]+=*)`)
	operationCookiePattern           = regexp.MustCompile(`(?i)((?:Cookie|Set-Cookie)\s*[:=]\s*)([^\r\n]+)`)
	operationAuthorizationPattern    = regexp.MustCompile(`(?i)(Authorization\s*[:=]\s*(?:Bearer\s+)?)([^\r\n]+)`)
	operationURLSecretQueryPattern   = regexp.MustCompile(`(?i)([?&](?:token|key|secret|password|signature|credential)=)([^\s&]+)`)
	operationDSNCredentialPattern    = regexp.MustCompile(`(?i)(\b(?:postgres|postgresql|mysql|redis|mongodb)://[^\s:@/]+:)([^\s@]+)(@)`)
	operationJSONSecretPattern       = regexp.MustCompile(`(?i)("[^"]*(?:token|secret|password|passwd|api[_-]?key|database[_-]?url|dsn|authorization|cookie)[^"]*"\s*:\s*")([^"]*)(")`)
)

// loadAuthorizedOperation is the shared authorization boundary for every
// operation-derived API view. Operations do not carry a project ID, so a
// project-scoped principal must be confined through the operation's App.
func (s *Server) loadAuthorizedOperation(principal model.Principal, operationID string) (model.Operation, error) {
	op, err := s.store.GetOperation(strings.TrimSpace(operationID))
	if err != nil {
		return model.Operation{}, err
	}
	if principal.IsPlatformAdmin() {
		return op, nil
	}
	if strings.TrimSpace(op.TenantID) != strings.TrimSpace(principal.TenantID) {
		return model.Operation{}, errOperationNotVisible
	}
	if strings.TrimSpace(principal.ProjectID) == "" {
		return op, nil
	}
	if strings.TrimSpace(op.AppID) == "" {
		return model.Operation{}, errOperationNotVisible
	}
	app, err := s.store.GetApp(op.AppID)
	if err != nil || !principalAllowsApp(principal, app) {
		return model.Operation{}, errOperationNotVisible
	}
	return op, nil
}

func (s *Server) writeOperationReadError(w http.ResponseWriter, err error) {
	if errors.Is(err, errOperationNotVisible) {
		httpx.WriteError(w, http.StatusForbidden, "operation is not visible to this tenant or project")
		return
	}
	s.writeStoreError(w, err)
}

func operationListProjectForPrincipal(principal model.Principal, requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	projectID := strings.TrimSpace(principal.ProjectID)
	if principal.IsPlatformAdmin() || projectID == "" {
		return requested, nil
	}
	if requested != "" && requested != projectID {
		return "", errOperationNotVisible
	}
	return projectID, nil
}

func redactOperationEvidenceForAPI(items []model.OperationEvidence, includePayload bool) []model.OperationEvidence {
	if len(items) == 0 {
		return []model.OperationEvidence{}
	}
	out := make([]model.OperationEvidence, len(items))
	for index, item := range items {
		out[index] = item
		out[index].Summary = redactOperationDiagnosticString(item.Summary)
		out[index].Message = redactOperationDiagnosticString(item.Message)
		out[index].Reason = redactOperationDiagnosticString(item.Reason)
		if includePayload {
			out[index].Payload = redactOperationDiagnosticMap(item.Payload)
		} else {
			out[index].Payload = nil
		}
	}
	return out
}

func redactOperationTimelineForAPI(items []model.OperationTimelineEntry, includePayload bool) []model.OperationTimelineEntry {
	if len(items) == 0 {
		return []model.OperationTimelineEntry{}
	}
	out := make([]model.OperationTimelineEntry, len(items))
	for index, item := range items {
		out[index] = item
		out[index].Summary = redactOperationDiagnosticString(item.Summary)
		out[index].Message = redactOperationDiagnosticString(item.Message)
		out[index].Reason = redactOperationDiagnosticString(item.Reason)
		if includePayload {
			out[index].Payload = redactOperationDiagnosticMap(item.Payload)
		} else {
			out[index].Payload = nil
		}
	}
	return out
}

func redactOperationDiagnosisForAPI(in model.OperationDiagnosis) model.OperationDiagnosis {
	out := in
	out.Summary = redactOperationDiagnosticString(in.Summary)
	out.Hint = redactOperationDiagnosticString(in.Hint)
	out.Evidence = redactOperationDiagnosticStrings(in.Evidence)
	out.RecommendedNextActions = redactOperationDiagnosticStrings(in.RecommendedNextActions)
	out.DependencyChain = redactOperationDiagnosticStrings(in.DependencyChain)
	if in.ConfirmedCause != nil {
		cause := *in.ConfirmedCause
		cause.Message = redactOperationDiagnosticString(cause.Message)
		cause.Reason = redactOperationDiagnosticString(cause.Reason)
		out.ConfirmedCause = &cause
	}
	if in.ProbableCause != nil {
		cause := *in.ProbableCause
		cause.Message = redactOperationDiagnosticString(cause.Message)
		cause.Reason = redactOperationDiagnosticString(cause.Reason)
		out.ProbableCause = &cause
	}
	if in.BuilderPlacement != nil {
		placement := *in.BuilderPlacement
		placement.RequiredNodeLabels = redactOperationDiagnosticStringMap(in.BuilderPlacement.RequiredNodeLabels)
		placement.Nodes = append([]model.BuilderPlacementNodeInspection(nil), in.BuilderPlacement.Nodes...)
		for index := range placement.Nodes {
			placement.Nodes[index].Reasons = redactOperationDiagnosticStrings(placement.Nodes[index].Reasons)
		}
		out.BuilderPlacement = &placement
	}
	return out
}

func redactOperationDebugBundleForAPI(in model.OperationDebugBundle) model.OperationDebugBundle {
	data, err := json.Marshal(in)
	if err != nil {
		return model.OperationDebugBundle{Operation: redactOperationForDebugBundle(in.Operation)}
	}
	var normalized map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return model.OperationDebugBundle{Operation: redactOperationForDebugBundle(in.Operation)}
	}
	redacted := redactOperationDiagnosticValue(normalized, "", false)
	data, err = json.Marshal(redacted)
	if err != nil {
		return model.OperationDebugBundle{Operation: redactOperationForDebugBundle(in.Operation)}
	}
	var out model.OperationDebugBundle
	if err := json.Unmarshal(data, &out); err != nil {
		return model.OperationDebugBundle{Operation: redactOperationForDebugBundle(in.Operation)}
	}
	return out
}

func redactOperationDiagnosticStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, len(values))
	for index, value := range values {
		out[index] = redactOperationDiagnosticString(value)
	}
	return out
}

func redactOperationDiagnosticStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if operationDiagnosticKeyLooksSensitive(key) {
			out[key] = redactOperationSecretString(value)
			continue
		}
		out[key] = redactOperationDiagnosticString(value)
	}
	return out
}

func redactOperationDiagnosticMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	// Normalize typed maps/slices to JSON values before walking them. Evidence
	// payloads may come directly from in-memory controller structs or from JSONB.
	data, err := json.Marshal(values)
	if err != nil {
		return map[string]any{"redaction_error": "payload omitted"}
	}
	var normalized map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return map[string]any{"redaction_error": "payload omitted"}
	}
	redacted, ok := redactOperationDiagnosticValue(normalized, "", false).(map[string]any)
	if !ok {
		return map[string]any{"redaction_error": "payload omitted"}
	}
	return redacted
}

func redactOperationDiagnosticValue(value any, key string, force bool) any {
	if force {
		return redactOperationSecretValue(value)
	}
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		secretEntry := operationDiagnosticSecretEntry(typed)
		for childKey, child := range typed {
			normalizedKey := normalizeOperationDiagnosticKey(childKey)
			switch {
			case normalizedKey == "secret" && operationDiagnosticBoolean(child):
				out[childKey] = child
			case secretEntry && (normalizedKey == "content" || normalizedKey == "seedcontent" || normalizedKey == "value"):
				out[childKey] = redactOperationSecretValue(child)
			case operationDiagnosticKeyLooksSensitive(childKey):
				out[childKey] = redactOperationSecretValue(child)
			case normalizedKey == "env" || normalizedKey == "environment" || normalizedKey == "environmentvariables":
				out[childKey] = redactOperationEnvValue(child)
			case operationDiagnosticKeyRedactsChildren(childKey):
				out[childKey] = redactOperationSecretValue(child)
			default:
				out[childKey] = redactOperationDiagnosticValue(child, childKey, false)
			}
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = redactOperationDiagnosticValue(child, key, false)
		}
		return out
	case string:
		return redactOperationDiagnosticString(typed)
	default:
		return typed
	}
}

func redactOperationEnvValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			out[key] = redactOperationSecretValue(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			entry, ok := child.(map[string]any)
			if !ok {
				out[index] = redactOperationSecretValue(child)
				continue
			}
			redactedEntry := make(map[string]any, len(entry))
			for key, entryValue := range entry {
				switch normalizeOperationDiagnosticKey(key) {
				case "name":
					redactedEntry[key] = redactOperationDiagnosticValue(entryValue, key, false)
				case "value", "valuefrom":
					redactedEntry[key] = redactOperationSecretValue(entryValue)
				default:
					redactedEntry[key] = redactOperationDiagnosticValue(entryValue, key, false)
				}
			}
			out[index] = redactedEntry
		}
		return out
	default:
		return redactOperationSecretValue(value)
	}
}

func redactOperationSecretValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			out[key] = redactOperationSecretValue(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = redactOperationSecretValue(child)
		}
		return out
	case string:
		return redactOperationSecretString(typed)
	case nil:
		return nil
	default:
		return apiRedactedSecretValue
	}
}

func redactOperationSecretString(value string) string {
	if strings.TrimSpace(value) == "" {
		return value
	}
	return apiRedactedSecretValue
}

func redactOperationDiagnosticString(value string) string {
	if strings.TrimSpace(value) == "" {
		return value
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Host != "" && parsed.User != nil {
		parsed.User = url.User("redacted")
		value = parsed.String()
	}
	value = operationSecretAssignmentPattern.ReplaceAllString(value, `${1}[redacted]`)
	value = operationAuthorizationPattern.ReplaceAllString(value, `${1}[redacted]`)
	value = operationBearerPattern.ReplaceAllString(value, `${1}[redacted]`)
	value = operationCookiePattern.ReplaceAllString(value, `${1}[redacted]`)
	value = operationURLSecretQueryPattern.ReplaceAllString(value, `${1}[redacted]`)
	value = operationDSNCredentialPattern.ReplaceAllString(value, `${1}[redacted]${3}`)
	value = operationJSONSecretPattern.ReplaceAllString(value, `${1}[redacted]${3}`)
	return value
}

func operationDiagnosticSecretEntry(values map[string]any) bool {
	for key, value := range values {
		if normalizeOperationDiagnosticKey(key) == "secret" && operationDiagnosticBoolean(value) {
			return true
		}
	}
	return false
}

func operationDiagnosticBoolean(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func normalizeOperationDiagnosticKey(key string) string {
	normalized := strings.ToLower(strings.TrimSpace(key))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, ".", "")
	return normalized
}

func operationDiagnosticKeyLooksSensitive(key string) bool {
	normalized := normalizeOperationDiagnosticKey(key)
	for _, marker := range []string{
		"authorization",
		"accesstoken",
		"refreshtoken",
		"token",
		"apikey",
		"secret",
		"password",
		"passwd",
		"credential",
		"cookie",
		"databaseurl",
		"dburl",
		"dsn",
		"connectionstring",
		"connectionurl",
		"postgresurl",
		"postgresqlurl",
		"privatekey",
		"accesskey",
		"restarttoken",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func operationDiagnosticKeyRedactsChildren(key string) bool {
	switch normalizeOperationDiagnosticKey(key) {
	case "data", "stringdata":
		return true
	default:
		return false
	}
}
