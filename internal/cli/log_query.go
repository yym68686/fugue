package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const (
	logsQuerySchemaVersion    = "fugue.logs-query.v1"
	defaultLogsQueryTailLines = 2000
	defaultLogsQueryLimit     = 200
)

var logfmtFieldPattern = regexp.MustCompile(`([A-Za-z0-9_.:-]+)=("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|[^ \t]+)`)

type logsQueryOptions struct {
	Component string
	Pod       string
	Container string
	RequestID string
	Method    string
	Path      string
	Status    string
	Since     string
	Until     string
	TailLines int
	Limit     int
	Previous  bool
}

type logsQueryFilters struct {
	RequestID string `json:"request_id,omitempty"`
	Method    string `json:"method,omitempty"`
	Path      string `json:"path,omitempty"`
	Status    string `json:"status,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
}

type logsQueryEntry struct {
	Timestamp string `json:"timestamp,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
	Stream    string `json:"stream,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Method    string `json:"method,omitempty"`
	Path      string `json:"path,omitempty"`
	Status    string `json:"status,omitempty"`
	Phase     string `json:"phase,omitempty"`
	Message   string `json:"message"`
}

type logsQueryCorrelation struct {
	RequestID    string   `json:"request_id"`
	StartAt      string   `json:"start_at,omitempty"`
	HeadersAt    string   `json:"headers_at,omitempty"`
	FirstBodyAt  string   `json:"first_body_at,omitempty"`
	FirstSSEAt   string   `json:"first_sse_at,omitempty"`
	EndAt        string   `json:"end_at,omitempty"`
	ExceptionAt  string   `json:"exception_at,omitempty"`
	PhaseOrder   []string `json:"phase_order,omitempty"`
	MissingPhase []string `json:"missing_phases,omitempty"`
	EntryCount   int      `json:"entry_count"`
}

type logsQueryResult struct {
	SchemaVersion       string                 `json:"schema_version"`
	App                 string                 `json:"app"`
	AppID               string                 `json:"app_id"`
	Component           string                 `json:"component"`
	Namespace           string                 `json:"namespace,omitempty"`
	Selector            string                 `json:"selector,omitempty"`
	Container           string                 `json:"container,omitempty"`
	Since               string                 `json:"since,omitempty"`
	Until               string                 `json:"until,omitempty"`
	Filters             logsQueryFilters       `json:"filters,omitempty"`
	BackendStatus       string                 `json:"backend_status"`
	ResultStatus        string                 `json:"result_status"`
	Summary             string                 `json:"summary"`
	MatchedEntries      int                    `json:"matched_entries"`
	ReturnedEntries     int                    `json:"returned_entries"`
	Truncated           bool                   `json:"truncated,omitempty"`
	Entries             []logsQueryEntry       `json:"entries,omitempty"`
	RequestCorrelations []logsQueryCorrelation `json:"request_correlations,omitempty"`
	Warnings            []string               `json:"warnings,omitempty"`
}

type logsQueryTimeWindow struct {
	Since *time.Time
	Until *time.Time
}

type logsQueryRecord struct {
	Entry        logsQueryEntry
	Timestamp    time.Time
	HasTimestamp bool
	Order        int
}

func (c *CLI) newLogsQueryCommand() *cobra.Command {
	opts := logsQueryOptions{
		Component: "app",
		TailLines: defaultLogsQueryTailLines,
		Limit:     defaultLogsQueryLimit,
	}
	cmd := &cobra.Command{
		Use:   "query <app>",
		Short: "Query structured runtime log entries with request and HTTP field filters",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			result, runErr := c.runLogsQuery(client, app, opts)
			sanitized := sanitizeLogsQueryResult(result, c.shouldRedact())
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, sanitized); err != nil {
					return err
				}
			} else {
				if err := renderLogsQueryResult(c.stdout, sanitized); err != nil {
					return err
				}
			}
			if runErr != nil {
				return runErr
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.Component, "component", opts.Component, "Runtime component: app or postgres")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Limit entries to one pod")
	cmd.Flags().StringVar(&opts.Container, "container", "", "Limit entries to one container name")
	cmd.Flags().StringVar(&opts.RequestID, "request-id", "", "Limit entries to one request or trace identifier")
	cmd.Flags().StringVar(&opts.Method, "method", "", "Limit entries to one HTTP method")
	cmd.Flags().StringVar(&opts.Path, "path", "", "Limit entries to one HTTP path")
	cmd.Flags().StringVar(&opts.Status, "status", "", "Limit entries to one HTTP status code")
	cmd.Flags().StringVar(&opts.Since, "since", "", "Lower time bound as RFC3339 or relative duration like 30m")
	cmd.Flags().StringVar(&opts.Until, "until", "", "Upper time bound as RFC3339 or relative duration like 5m")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Maximum runtime log lines to snapshot before local filtering")
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum structured entries to return after filtering")
	cmd.Flags().BoolVar(&opts.Previous, "previous", false, "Read the previous container instance logs instead of the current one")
	return cmd
}

func (c *CLI) runLogsQuery(client *Client, app model.App, opts logsQueryOptions) (logsQueryResult, error) {
	window, err := resolveLogsQueryTimeWindow(opts.Since, opts.Until, time.Now().UTC())
	if err != nil {
		return logsQueryResult{}, withExitCode(err, ExitCodeUserInput)
	}
	component := strings.TrimSpace(strings.ToLower(opts.Component))
	if component == "" {
		component = "app"
	}
	if component != "app" && component != "postgres" {
		return logsQueryResult{}, withExitCode(fmt.Errorf("--component must be app or postgres"), ExitCodeUserInput)
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultLogsQueryLimit
	}
	tailLines := opts.TailLines
	if tailLines <= 0 {
		tailLines = defaultLogsQueryTailLines
	}

	result := logsQueryResult{
		SchemaVersion: logsQuerySchemaVersion,
		App:           strings.TrimSpace(app.Name),
		AppID:         strings.TrimSpace(app.ID),
		Component:     component,
		Since:         strings.TrimSpace(opts.Since),
		Until:         strings.TrimSpace(opts.Until),
		Filters: logsQueryFilters{
			RequestID: strings.TrimSpace(opts.RequestID),
			Method:    strings.ToUpper(strings.TrimSpace(opts.Method)),
			Path:      strings.TrimSpace(opts.Path),
			Status:    strings.TrimSpace(opts.Status),
			Pod:       strings.TrimSpace(opts.Pod),
			Container: strings.TrimSpace(opts.Container),
		},
		BackendStatus: "ok",
		ResultStatus:  "empty",
		Warnings:      []string{},
	}

	records, namespace, selector, container, streamWarnings, streamErr := collectLogsQueryRuntimeRecords(client, app.ID, runtimeLogsOptions{
		Component: component,
		Pod:       strings.TrimSpace(opts.Pod),
		TailLines: tailLines,
		Previous:  opts.Previous,
	}, window)
	result.Namespace = namespace
	result.Selector = selector
	result.Container = container
	result.Warnings = append(result.Warnings, streamWarnings...)

	if streamErr != nil {
		if isLogsQueryEmptyStreamError(streamErr) {
			result.ResultStatus = "empty"
			result.Summary = "no runtime log entries matched because no live pods were available"
			return result, nil
		}
		result.BackendStatus = "unavailable"
		result.ResultStatus = "backend_unavailable"
		result.Summary = "the runtime log backend is unavailable"
		result.Warnings = append(result.Warnings, strings.TrimSpace(streamErr.Error()))
		return result, withExitCode(errors.New(result.Summary), ExitCodeSystemFault)
	}

	filtered := filterLogsQueryRecords(records, result.Filters)
	result.RequestCorrelations = buildLogsQueryCorrelations(filtered, result.Filters.RequestID)
	result.MatchedEntries = len(filtered)
	if len(filtered) == 0 {
		result.ResultStatus = "empty"
		result.Summary = "no runtime log entries matched the requested filters"
		if len(records) == 0 && len(result.Warnings) > 0 {
			result.BackendStatus = "unavailable"
			result.ResultStatus = "backend_unavailable"
			result.Summary = "the runtime log backend returned no readable entries"
			return result, withExitCode(errors.New(result.Summary), ExitCodeSystemFault)
		}
		return result, nil
	}

	result.ResultStatus = "ok"
	if len(filtered) > limit {
		result.Truncated = true
		filtered = filtered[:limit]
	}
	result.ReturnedEntries = len(filtered)
	result.Entries = make([]logsQueryEntry, 0, len(filtered))
	for _, record := range filtered {
		result.Entries = append(result.Entries, record.Entry)
	}
	switch {
	case result.Truncated:
		result.Summary = fmt.Sprintf("matched %d runtime log entries and returned the first %d after filtering", result.MatchedEntries, result.ReturnedEntries)
	default:
		result.Summary = fmt.Sprintf("matched %d runtime log entries", result.MatchedEntries)
	}
	return result, nil
}

func collectLogsQueryRuntimeRecords(client *Client, appID string, opts runtimeLogsOptions, window logsQueryTimeWindow) ([]logsQueryRecord, string, string, string, []string, error) {
	records := make([]logsQueryRecord, 0, opts.TailLines)
	warnings := make([]string, 0)
	namespace := ""
	selector := ""
	container := ""
	order := 0
	err := client.StreamRuntimeLogs(appID, opts, false, func(event sseEvent) error {
		switch event.Event {
		case "ready":
			var payload runtimeLogStreamReadyEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			namespace = strings.TrimSpace(payload.Namespace)
			selector = strings.TrimSpace(payload.Selector)
			container = strings.TrimSpace(payload.Container)
		case "state":
			var payload runtimeLogStreamStateEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			namespace = firstNonEmptyTrimmed(namespace, strings.TrimSpace(payload.Namespace))
			selector = firstNonEmptyTrimmed(selector, strings.TrimSpace(payload.Selector))
			container = firstNonEmptyTrimmed(container, strings.TrimSpace(payload.Container))
		case "warning":
			var payload logStreamWarningEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			warnings = append(warnings, strings.TrimSpace(payload.Message))
		case "log":
			var payload logStreamLogEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			record := buildLogsQueryRecord(payload, order)
			order++
			if !recordMatchesLogsQueryTimeWindow(record, window) {
				return nil
			}
			records = append(records, record)
		}
		return nil
	})
	sortLogsQueryRecords(records)
	return records, namespace, selector, container, uniqueSortedStrings(warnings), err
}

func buildLogsQueryRecord(payload logStreamLogEvent, order int) logsQueryRecord {
	record := logsQueryRecord{
		Entry: logsQueryEntry{
			Pod:       strings.TrimSpace(payload.Source.Pod),
			Container: strings.TrimSpace(payload.Source.Container),
			Stream:    firstNonEmptyTrimmed(strings.TrimSpace(payload.Source.Stream), "runtime"),
			Message:   strings.TrimSpace(payload.Line),
		},
		Order: order,
	}
	if timestamp, ok := parseLogsQueryTimestamp(payload.Timestamp); ok {
		record.Timestamp = timestamp
		record.HasTimestamp = true
		record.Entry.Timestamp = timestamp.Format(time.RFC3339Nano)
	}

	line := strings.TrimSpace(payload.Line)
	if timestamp, body, ok := parseLeadingRFC3339LogTimestamp(line); ok {
		if !record.HasTimestamp {
			record.Timestamp = timestamp
			record.HasTimestamp = true
			record.Entry.Timestamp = timestamp.Format(time.RFC3339Nano)
		}
		line = strings.TrimSpace(body)
	}

	fields := map[string]string{}
	message := ""
	if jsonFields, jsonMessage, ok := parseLogsQueryJSONFields(line); ok {
		mergeLogsQueryFields(fields, jsonFields)
		message = firstNonEmptyTrimmed(message, jsonMessage)
	}
	mergeLogsQueryFields(fields, parseLogsQueryLogfmtFields(line))
	if !record.HasTimestamp {
		if timestamp, ok := parseLogsQueryFieldTimestamp(fields); ok {
			record.Timestamp = timestamp
			record.HasTimestamp = true
			record.Entry.Timestamp = timestamp.Format(time.RFC3339Nano)
		}
	}

	record.Entry.RequestID = firstNonEmptyTrimmed(
		fields["requestid"],
		fields["request_id"],
		fields["reqid"],
		fields["req_id"],
		fields["xrequestid"],
		fields["traceid"],
		fields["trace_id"],
	)
	record.Entry.Method = strings.ToUpper(strings.TrimSpace(firstNonEmptyTrimmed(
		fields["method"],
		fields["httpmethod"],
		fields["requestmethod"],
		fields["request_method"],
	)))
	record.Entry.Path = firstNonEmptyTrimmed(
		fields["path"],
		fields["requestpath"],
		fields["request_path"],
		fields["uri"],
		fields["requesturi"],
		fields["request_uri"],
		fields["urlpath"],
		fields["url_path"],
	)
	record.Entry.Status = normalizeLogsQueryStatus(firstNonEmptyTrimmed(
		fields["status"],
		fields["statuscode"],
		fields["status_code"],
		fields["httpstatus"],
		fields["http_status"],
		fields["httpstatuscode"],
		fields["http_status_code"],
		fields["code"],
	))
	record.Entry.Stream = firstNonEmptyTrimmed(record.Entry.Stream, fields["stream"])
	record.Entry.Pod = firstNonEmptyTrimmed(record.Entry.Pod, fields["pod"])
	record.Entry.Container = firstNonEmptyTrimmed(record.Entry.Container, fields["container"])
	record.Entry.Message = firstNonEmptyTrimmed(
		message,
		fields["message"],
		fields["msg"],
		fields["log"],
		fields["line"],
		record.Entry.Message,
		line,
	)
	record.Entry.Phase = detectLogsQueryPhase(fields, record.Entry.Message)
	return record
}

func filterLogsQueryRecords(records []logsQueryRecord, filters logsQueryFilters) []logsQueryRecord {
	if len(records) == 0 {
		return nil
	}
	out := make([]logsQueryRecord, 0, len(records))
	for _, record := range records {
		if filters.RequestID != "" && !strings.EqualFold(strings.TrimSpace(record.Entry.RequestID), filters.RequestID) {
			continue
		}
		if filters.Method != "" && !strings.EqualFold(strings.TrimSpace(record.Entry.Method), filters.Method) {
			continue
		}
		if filters.Path != "" && !strings.EqualFold(strings.TrimSpace(record.Entry.Path), filters.Path) {
			continue
		}
		if filters.Status != "" && !strings.EqualFold(strings.TrimSpace(record.Entry.Status), filters.Status) {
			continue
		}
		if filters.Pod != "" && !strings.EqualFold(strings.TrimSpace(record.Entry.Pod), filters.Pod) {
			continue
		}
		if filters.Container != "" && !strings.EqualFold(strings.TrimSpace(record.Entry.Container), filters.Container) {
			continue
		}
		out = append(out, record)
	}
	return out
}

func sortLogsQueryRecords(records []logsQueryRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		left, right := records[i], records[j]
		switch {
		case left.HasTimestamp && right.HasTimestamp:
			if !left.Timestamp.Equal(right.Timestamp) {
				return left.Timestamp.Before(right.Timestamp)
			}
		case left.HasTimestamp != right.HasTimestamp:
			return left.HasTimestamp
		}
		return left.Order < right.Order
	})
}

func buildLogsQueryCorrelations(records []logsQueryRecord, requestIDFilter string) []logsQueryCorrelation {
	grouped := map[string][]logsQueryRecord{}
	for _, record := range records {
		requestID := strings.TrimSpace(record.Entry.RequestID)
		if requestID == "" {
			continue
		}
		if requestIDFilter != "" && !strings.EqualFold(requestID, requestIDFilter) {
			continue
		}
		grouped[requestID] = append(grouped[requestID], record)
	}
	if len(grouped) == 0 {
		return nil
	}
	requestIDs := make([]string, 0, len(grouped))
	for requestID := range grouped {
		requestIDs = append(requestIDs, requestID)
	}
	sort.Strings(requestIDs)

	out := make([]logsQueryCorrelation, 0, len(requestIDs))
	for _, requestID := range requestIDs {
		records := grouped[requestID]
		correlation := logsQueryCorrelation{
			RequestID:  requestID,
			EntryCount: len(records),
		}
		phaseOrder := make([]string, 0, len(records))
		seenPhases := map[string]struct{}{}
		for _, record := range records {
			phase := strings.TrimSpace(record.Entry.Phase)
			if phase == "" {
				continue
			}
			if _, ok := seenPhases[phase]; !ok {
				seenPhases[phase] = struct{}{}
				phaseOrder = append(phaseOrder, phase)
			}
			timestamp := strings.TrimSpace(record.Entry.Timestamp)
			switch phase {
			case "start":
				correlation.StartAt = firstNonEmptyTrimmed(correlation.StartAt, timestamp)
			case "headers":
				correlation.HeadersAt = firstNonEmptyTrimmed(correlation.HeadersAt, timestamp)
			case "first_body":
				correlation.FirstBodyAt = firstNonEmptyTrimmed(correlation.FirstBodyAt, timestamp)
			case "first_sse":
				correlation.FirstSSEAt = firstNonEmptyTrimmed(correlation.FirstSSEAt, timestamp)
			case "end":
				correlation.EndAt = firstNonEmptyTrimmed(correlation.EndAt, timestamp)
			case "exception":
				correlation.ExceptionAt = firstNonEmptyTrimmed(correlation.ExceptionAt, timestamp)
			}
		}
		correlation.PhaseOrder = phaseOrder
		for _, required := range []struct {
			Name  string
			Value string
		}{
			{Name: "start", Value: correlation.StartAt},
			{Name: "first_body", Value: correlation.FirstBodyAt},
			{Name: "end", Value: correlation.EndAt},
			{Name: "exception", Value: correlation.ExceptionAt},
		} {
			if strings.TrimSpace(required.Value) == "" {
				correlation.MissingPhase = append(correlation.MissingPhase, required.Name)
			}
		}
		out = append(out, correlation)
	}
	return out
}

func resolveLogsQueryTimeWindow(sinceRaw, untilRaw string, now time.Time) (logsQueryTimeWindow, error) {
	window := logsQueryTimeWindow{}
	if since, ok, err := parseDiagnosticTimeWindowValue(sinceRaw, now); err != nil {
		return logsQueryTimeWindow{}, err
	} else if ok {
		window.Since = &since
	}
	if until, ok, err := parseDiagnosticTimeWindowValue(untilRaw, now); err != nil {
		return logsQueryTimeWindow{}, err
	} else if ok {
		window.Until = &until
	}
	if window.Since != nil && window.Until != nil && window.Since.After(*window.Until) {
		return logsQueryTimeWindow{}, fmt.Errorf("--since must be earlier than or equal to --until")
	}
	return window, nil
}

func recordMatchesLogsQueryTimeWindow(record logsQueryRecord, window logsQueryTimeWindow) bool {
	if window.Since == nil && window.Until == nil {
		return true
	}
	if !record.HasTimestamp {
		return false
	}
	if window.Since != nil && record.Timestamp.Before(window.Since.UTC()) {
		return false
	}
	if window.Until != nil && record.Timestamp.After(window.Until.UTC()) {
		return false
	}
	return true
}

func parseLogsQueryJSONFields(raw string) (map[string]string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil, "", false
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, "", false
	}
	root, ok := decoded.(map[string]any)
	if !ok {
		return nil, "", false
	}
	fields := map[string]string{}
	flattenLogsQueryObject("", root, fields, 0)
	message := firstNonEmptyTrimmed(
		fields["message"],
		fields["msg"],
		fields["log"],
		fields["line"],
	)
	return fields, message, true
}

func flattenLogsQueryObject(prefix string, value any, out map[string]string, depth int) {
	if depth > 1 {
		if prefix == "" {
			return
		}
		if text := stringifyLogsQueryField(value); text != "" {
			out[normalizeLogsQueryFieldKey(prefix)] = text
		}
		return
	}
	object, ok := value.(map[string]any)
	if !ok {
		if prefix == "" {
			return
		}
		if text := stringifyLogsQueryField(value); text != "" {
			out[normalizeLogsQueryFieldKey(prefix)] = text
		}
		return
	}
	for key, nested := range object {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		fieldKey := key
		if prefix != "" {
			fieldKey = prefix + "." + key
		}
		switch typed := nested.(type) {
		case map[string]any:
			flattenLogsQueryObject(fieldKey, typed, out, depth+1)
		default:
			if text := stringifyLogsQueryField(typed); text != "" {
				out[normalizeLogsQueryFieldKey(fieldKey)] = text
			}
		}
	}
}

func stringifyLogsQueryField(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case float64:
		if typed == float64(int64(typed)) {
			return fmt.Sprintf("%d", int64(typed))
		}
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	case bool:
		return fmt.Sprintf("%t", typed)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func parseLogsQueryLogfmtFields(raw string) map[string]string {
	out := map[string]string{}
	for _, match := range logfmtFieldPattern.FindAllStringSubmatch(raw, -1) {
		key := normalizeLogsQueryFieldKey(match[1])
		if key == "" {
			continue
		}
		value := strings.TrimSpace(match[2])
		value = strings.Trim(value, `"'`)
		if value == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func normalizeLogsQueryFieldKey(raw string) string {
	replacer := strings.NewReplacer(".", "", "_", "", "-", "", ":", "", "@", "")
	return strings.ToLower(replacer.Replace(strings.TrimSpace(raw)))
}

func mergeLogsQueryFields(target, source map[string]string) {
	for key, value := range source {
		key = normalizeLogsQueryFieldKey(key)
		if key == "" || strings.TrimSpace(value) == "" {
			continue
		}
		if _, exists := target[key]; exists {
			continue
		}
		target[key] = strings.TrimSpace(value)
	}
}

func parseLogsQueryFieldTimestamp(fields map[string]string) (time.Time, bool) {
	for _, key := range []string{
		"timestamp",
		"time",
		"ts",
		"at",
		"eventtime",
	} {
		if timestamp, ok := parseLogsQueryTimestamp(fields[key]); ok {
			return timestamp, true
		}
	}
	return time.Time{}, false
}

func parseLogsQueryTimestamp(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(strings.Trim(raw, `"'`))
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func parseLeadingRFC3339LogTimestamp(raw string) (time.Time, string, bool) {
	raw = strings.TrimRight(raw, "\r")
	parts := strings.SplitN(raw, " ", 2)
	if len(parts) != 2 {
		return time.Time{}, raw, false
	}
	timestamp, ok := parseLogsQueryTimestamp(parts[0])
	if !ok {
		return time.Time{}, raw, false
	}
	return timestamp.UTC(), parts[1], true
}

func normalizeLogsQueryStatus(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return raw
}

func detectLogsQueryPhase(fields map[string]string, message string) string {
	candidates := []string{
		fields["phase"],
		fields["event"],
		fields["action"],
		fields["kind"],
		fields["state"],
		message,
	}
	for _, candidate := range candidates {
		normalized := strings.ToLower(strings.TrimSpace(candidate))
		if normalized == "" {
			continue
		}
		switch {
		case containsLogsQueryToken(normalized, "first body"), containsLogsQueryToken(normalized, "first_body"), containsLogsQueryToken(normalized, "first-byte"), containsLogsQueryToken(normalized, "first byte"), containsLogsQueryToken(normalized, "first chunk"), containsLogsQueryToken(normalized, "ttfb"):
			return "first_body"
		case containsLogsQueryToken(normalized, "first sse"), containsLogsQueryToken(normalized, "first_sse"), containsLogsQueryToken(normalized, "sse event"):
			return "first_sse"
		case containsLogsQueryToken(normalized, "headers"), containsLogsQueryToken(normalized, "header_sent"), containsLogsQueryToken(normalized, "headers sent"), containsLogsQueryToken(normalized, "response headers"):
			return "headers"
		case containsLogsQueryToken(normalized, "start"), containsLogsQueryToken(normalized, "begin"), containsLogsQueryToken(normalized, "request received"), containsLogsQueryToken(normalized, "incoming request"):
			return "start"
		case containsLogsQueryToken(normalized, "exception"), containsLogsQueryToken(normalized, "panic"), containsLogsQueryToken(normalized, "error"), containsLogsQueryToken(normalized, "failed"), containsLogsQueryToken(normalized, "timeout"), containsLogsQueryToken(normalized, "stall"):
			return "exception"
		case containsLogsQueryToken(normalized, "complete"), containsLogsQueryToken(normalized, "completed"), containsLogsQueryToken(normalized, "finish"), containsLogsQueryToken(normalized, "finished"), containsLogsQueryToken(normalized, "request end"), containsLogsQueryToken(normalized, "done"):
			return "end"
		}
	}
	return ""
}

func containsLogsQueryToken(value, token string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	token = strings.ToLower(strings.TrimSpace(token))
	if value == "" || token == "" {
		return false
	}
	return strings.Contains(value, token)
}

func renderLogsQueryResult(w io.Writer, result logsQueryResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "app", Value: formatDisplayName(result.App, result.AppID, false)},
		kvPair{Key: "component", Value: result.Component},
		kvPair{Key: "namespace", Value: result.Namespace},
		kvPair{Key: "selector", Value: result.Selector},
		kvPair{Key: "container", Value: result.Container},
		kvPair{Key: "since", Value: result.Since},
		kvPair{Key: "until", Value: result.Until},
		kvPair{Key: "backend_status", Value: result.BackendStatus},
		kvPair{Key: "result_status", Value: result.ResultStatus},
		kvPair{Key: "matched_entries", Value: formatInt(result.MatchedEntries)},
		kvPair{Key: "returned_entries", Value: formatInt(result.ReturnedEntries)},
		kvPair{Key: "truncated", Value: fmt.Sprintf("%t", result.Truncated)},
		kvPair{Key: "summary", Value: result.Summary},
	); err != nil {
		return err
	}
	if filtersText := formatLogsQueryFilters(result.Filters); filtersText != "" {
		if _, err := fmt.Fprintf(w, "filters=%s\n", filtersText); err != nil {
			return err
		}
	}
	for _, warning := range result.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	if len(result.Entries) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeLogsQueryEntryTable(w, result.Entries); err != nil {
			return err
		}
	}
	if len(result.RequestCorrelations) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "request_correlations"); err != nil {
			return err
		}
		if err := writeLogsQueryCorrelationTable(w, result.RequestCorrelations); err != nil {
			return err
		}
	}
	return nil
}

func writeLogsQueryEntryTable(w io.Writer, entries []logsQueryEntry) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TIME\tPOD\tCONTAINER\tSTREAM\tREQUEST_ID\tMETHOD\tPATH\tSTATUS\tPHASE\tMESSAGE"); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			entry.Timestamp,
			entry.Pod,
			entry.Container,
			entry.Stream,
			entry.RequestID,
			entry.Method,
			entry.Path,
			entry.Status,
			entry.Phase,
			entry.Message,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeLogsQueryCorrelationTable(w io.Writer, correlations []logsQueryCorrelation) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "REQUEST_ID\tSTART\tHEADERS\tFIRST_BODY\tFIRST_SSE\tEND\tEXCEPTION\tMISSING\tENTRIES"); err != nil {
		return err
	}
	for _, correlation := range correlations {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\n",
			correlation.RequestID,
			correlation.StartAt,
			correlation.HeadersAt,
			correlation.FirstBodyAt,
			correlation.FirstSSEAt,
			correlation.EndAt,
			correlation.ExceptionAt,
			strings.Join(correlation.MissingPhase, ","),
			correlation.EntryCount,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatLogsQueryFilters(filters logsQueryFilters) string {
	parts := make([]string, 0, 6)
	if filters.RequestID != "" {
		parts = append(parts, "request_id="+filters.RequestID)
	}
	if filters.Method != "" {
		parts = append(parts, "method="+filters.Method)
	}
	if filters.Path != "" {
		parts = append(parts, "path="+filters.Path)
	}
	if filters.Status != "" {
		parts = append(parts, "status="+filters.Status)
	}
	if filters.Pod != "" {
		parts = append(parts, "pod="+filters.Pod)
	}
	if filters.Container != "" {
		parts = append(parts, "container="+filters.Container)
	}
	return strings.Join(parts, ", ")
}

func isLogsQueryEmptyStreamError(err error) bool {
	return err != nil && strings.EqualFold(strings.TrimSpace(err.Error()), "no matching pods found")
}

func sanitizeLogsQueryResult(result logsQueryResult, redact bool) logsQueryResult {
	if !redact {
		return result
	}
	out := result
	out.Summary = redactDiagnosticString(out.Summary)
	out.Warnings = redactDiagnosticStringSlice(out.Warnings)
	for index := range out.Entries {
		out.Entries[index].Message = redactDiagnosticString(out.Entries[index].Message)
	}
	return out
}

func filterRawLogTextForTimeWindow(raw string, window logsQueryTimeWindow) string {
	if window.Since == nil && window.Until == nil {
		return raw
	}
	lines := strings.Split(raw, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		timestamp, body, ok := parseLeadingRFC3339LogTimestamp(line)
		if !ok {
			if timestamp, ok = parseLogsQueryLineTimestamp(line); !ok {
				continue
			} else {
				body = line
			}
		}
		if window.Since != nil && timestamp.Before(window.Since.UTC()) {
			continue
		}
		if window.Until != nil && timestamp.After(window.Until.UTC()) {
			continue
		}
		filtered = append(filtered, strings.TrimSpace(body))
	}
	return strings.Join(filtered, "\n")
}

func parseLogsQueryLineTimestamp(raw string) (time.Time, bool) {
	if timestamp, ok := parseLogsQueryTimestamp(raw); ok {
		return timestamp, true
	}
	if fields, _, ok := parseLogsQueryJSONFields(raw); ok {
		if timestamp, ok := parseLogsQueryFieldTimestamp(fields); ok {
			return timestamp, true
		}
	}
	if timestamp, ok := parseLogsQueryFieldTimestamp(parseLogsQueryLogfmtFields(raw)); ok {
		return timestamp, true
	}
	return time.Time{}, false
}
