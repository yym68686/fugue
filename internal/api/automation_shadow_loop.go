package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/automation"
	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/store"
)

const (
	defaultAutomationShadowLoopInterval = 30 * time.Second
	minAutomationShadowLoopInterval     = time.Second
	maxAutomationShadowLoopInterval     = 15 * time.Minute
	automationShadowLoopSettleDelay     = 5 * time.Second
	automationShadowLoopLockName        = "automation-shadow-control-loop"
	automationShadowLoopActorID         = "automation-shadow-control-loop"

	maxAutomationShadowPoliciesPerRun    = 2_000
	maxAutomationShadowEvaluationsPerRun = 4_096
	maxAutomationShadowCatchupWindows    = 16
	maxAutomationShadowOutcomeAggregates = 256
)

// AutomationShadowLoopConfig controls only observe-only evaluation and durable
// dispatch preparation. It cannot authorize an application mutation.
type AutomationShadowLoopConfig struct {
	Enabled  bool
	Interval time.Duration
}

type automationRequestOutcomeQueryFunc func(
	context.Context,
	string,
	[]int,
	time.Time,
	time.Time,
) ([]model.AutomationRequestOutcomeAggregate, string, error)

type automationShadowLoopRuntime struct {
	mu sync.Mutex

	leader       bool
	lastRun      time.Time
	lastSuccess  time.Time
	lastDuration time.Duration
	lastSummary  automationShadowLoopRunSummary
	lastError    string

	runCount                    int64
	errorCount                  int64
	leadershipContentionCount   int64
	policiesScannedCount        int64
	evaluationCount             int64
	matchCount                  int64
	intentCreatedCount          int64
	intentReusedCount           int64
	dispatchCreatedCount        int64
	dispatchReusedCount         int64
	ineligiblePolicyCount       int64
	policyLimitDeferredCount    int64
	catchupWindowSkippedCount   int64
	evaluationLimitSkippedCount int64
}

type automationShadowLoopRunSummary struct {
	PoliciesScanned        int
	ShadowPolicies         int
	PoliciesSelected       int
	IneligiblePolicies     int
	PolicyLimitDeferred    int
	Evaluations            int
	Matches                int
	IntentsCreated         int
	IntentsReused          int
	DispatchesCreated      int
	DispatchesReused       int
	CatchupWindowsSkipped  int
	EvaluationLimitSkipped int
	Errors                 int
}

type automationShadowLoopCursor map[string]time.Time

type automationShadowLoopErrorAccumulator struct {
	count int
	first error
}

func (a *automationShadowLoopErrorAccumulator) add(err error) {
	if err == nil {
		return
	}
	a.count++
	if a.first == nil {
		a.first = err
	}
}

func (a automationShadowLoopErrorAccumulator) err() error {
	if a.first == nil {
		return nil
	}
	return fmt.Errorf("%d automation shadow evaluation error(s); first: %w", a.count, a.first)
}

func normalizeAutomationShadowLoopConfig(cfg AutomationShadowLoopConfig) AutomationShadowLoopConfig {
	if cfg.Interval < minAutomationShadowLoopInterval ||
		cfg.Interval > maxAutomationShadowLoopInterval {
		cfg.Interval = defaultAutomationShadowLoopInterval
	}
	return cfg
}

func (s *Server) automationShadowLoopActive() bool {
	if s == nil || !s.automationShadowLoopConfig.Enabled {
		return false
	}
	status := s.observabilityConfig.Normalize().Status()
	return status.Enabled && status.AnalyticsConfigured
}

// StartBackgroundAutomationShadowLoop elects one API replica with a
// PostgreSQL advisory lock and keeps that lock for the leader lifetime. The
// loop creates observe-only intents and inert durable dispatch records; it has
// no execution path.
func (s *Server) StartBackgroundAutomationShadowLoop(ctx context.Context) {
	if s == nil || s.store == nil || !s.automationShadowLoopActive() {
		return
	}
	retryInterval := s.automationShadowLoopConfig.Interval
	if retryInterval > 5*time.Second {
		retryInterval = 5 * time.Second
	}
	if retryInterval < time.Second {
		retryInterval = time.Second
	}

	for {
		if ctx.Err() != nil {
			return
		}
		acquired, err := s.store.WithAdvisoryLock(
			ctx,
			automationShadowLoopLockName,
			func() error {
				s.setAutomationShadowLoopLeader(true)
				defer s.setAutomationShadowLoopLeader(false)
				s.runAutomationShadowLoopLeader(ctx)
				return nil
			},
		)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.recordAutomationShadowLoopLeadershipError(err)
			if s.log != nil {
				s.log.Printf("automation shadow loop leader election failed: %v", err)
			}
		} else if !acquired {
			s.recordAutomationShadowLoopContention()
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (s *Server) runAutomationShadowLoopLeader(ctx context.Context) {
	cursor := automationShadowLoopCursor{}
	for {
		started := time.Now().UTC()
		summary, nextInterval, err := s.runAutomationShadowLoopOnce(ctx, started, cursor)
		s.recordAutomationShadowLoopRun(started, time.Since(started), summary, err)
		if err != nil && s.log != nil {
			s.log.Printf(
				"automation shadow loop completed with errors: policies=%d shadow_policies=%d selected=%d deferred=%d evaluations=%d matches=%d intents_created=%d dispatches_created=%d duration=%s err=%v",
				summary.PoliciesScanned,
				summary.ShadowPolicies,
				summary.PoliciesSelected,
				summary.PolicyLimitDeferred,
				summary.Evaluations,
				summary.Matches,
				summary.IntentsCreated,
				summary.DispatchesCreated,
				time.Since(started),
				err,
			)
		} else if s.log != nil {
			s.log.Printf(
				"automation shadow loop complete: policies=%d shadow_policies=%d selected=%d deferred=%d evaluations=%d matches=%d intents_created=%d intents_reused=%d dispatches_created=%d dispatches_reused=%d duration=%s",
				summary.PoliciesScanned,
				summary.ShadowPolicies,
				summary.PoliciesSelected,
				summary.PolicyLimitDeferred,
				summary.Evaluations,
				summary.Matches,
				summary.IntentsCreated,
				summary.IntentsReused,
				summary.DispatchesCreated,
				summary.DispatchesReused,
				time.Since(started),
			)
		}
		if ctx.Err() != nil {
			return
		}
		if nextInterval <= 0 {
			nextInterval = s.automationShadowLoopConfig.Interval
		}
		timer := time.NewTimer(nextInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (s *Server) runAutomationShadowLoopOnce(
	ctx context.Context,
	now time.Time,
	cursor automationShadowLoopCursor,
) (automationShadowLoopRunSummary, time.Duration, error) {
	summary := automationShadowLoopRunSummary{}
	nextInterval := s.automationShadowLoopConfig.Interval
	if nextInterval <= 0 {
		nextInterval = defaultAutomationShadowLoopInterval
	}
	if cursor == nil {
		cursor = automationShadowLoopCursor{}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC().Truncate(time.Microsecond)

	policies, err := s.store.ListAutomationPolicies(store.AutomationPolicyFilter{PlatformAdmin: true})
	if err != nil {
		return summary, nextInterval, fmt.Errorf("list automation policies: %w", err)
	}
	summary.PoliciesScanned = len(policies)
	errorsSeen := automationShadowLoopErrorAccumulator{}

	activeCursorKeys := make(map[string]struct{})
	shadowPolicies := make([]model.AutomationPolicy, 0, len(policies))
	for _, policy := range policies {
		if policy.Mode != model.GatePolicyModeShadow {
			continue
		}
		summary.ShadowPolicies++
		shadowPolicies = append(shadowPolicies, policy)
		for _, rule := range policy.Rules {
			activeCursorKeys[automationShadowCursorKey(policy, rule)] = struct{}{}
			window, err := automationShadowRuleWindow(rule)
			if err == nil && window < nextInterval {
				nextInterval = window
			}
		}
	}
	policies, summary.PolicyLimitDeferred = automationShadowPolicyBatch(
		shadowPolicies,
		now,
		nextInterval,
	)
	summary.PoliciesSelected = len(policies)

	evaluationLimitReached := false
	evaluationLimitErrorRecorded := false
	for _, policy := range policies {
		if err := ctx.Err(); err != nil {
			return summary, nextInterval, err
		}
		app, err := s.store.GetApp(policy.Scope.ID)
		if err != nil {
			errorsSeen.add(fmt.Errorf("policy %s app lookup: %w", policy.ID, err))
			continue
		}
		if app.TenantID != policy.TenantID || app.ProjectID != policy.ProjectID {
			errorsSeen.add(fmt.Errorf("policy %s app ownership changed", policy.ID))
			continue
		}

		appEligible := automationShadowAppEligible(app)
		appRevision := ""
		readinessObservedAt := time.Time{}
		readiness := strings.TrimSpace(strings.ToLower(app.Status.Phase))
		if readiness == "" {
			readiness = "unknown"
		}
		if appEligible {
			appRevision, err = automation.AppRevisionHash(app.Spec)
			if err != nil {
				errorsSeen.add(fmt.Errorf("policy %s app revision: %w", policy.ID, err))
				continue
			}
			readinessObservedAt = app.Status.UpdatedAt.UTC()
			if readinessObservedAt.IsZero() {
				readinessObservedAt = app.UpdatedAt.UTC()
			}
			if readinessObservedAt.IsZero() {
				errorsSeen.add(fmt.Errorf("policy %s app readiness timestamp is unavailable", policy.ID))
				continue
			}
		}

		ineligiblePolicyRecorded := false
		for _, rule := range policy.Rules {
			window, err := automationShadowRuleWindow(rule)
			if err != nil {
				errorsSeen.add(fmt.Errorf("policy %s rule %s: %w", policy.ID, rule.ID, err))
				continue
			}
			if window < nextInterval {
				nextInterval = window
			}
			cursorKey := automationShadowCursorKey(policy, rule)
			windowEnds, skipped := automationShadowDueWindowEnds(
				cursor[cursorKey],
				now,
				window,
			)
			summary.CatchupWindowsSkipped += skipped
			if len(windowEnds) == 0 {
				continue
			}
			if !appEligible {
				cursor[cursorKey] = windowEnds[len(windowEnds)-1]
				if !ineligiblePolicyRecorded {
					summary.IneligiblePolicies++
					ineligiblePolicyRecorded = true
				}
				continue
			}

			if evaluationLimitReached {
				summary.EvaluationLimitSkipped += len(windowEnds)
				continue
			}
			for windowIndex, windowEnd := range windowEnds {
				if summary.Evaluations >= maxAutomationShadowEvaluationsPerRun {
					summary.EvaluationLimitSkipped += len(windowEnds) - windowIndex
					evaluationLimitReached = true
					if !evaluationLimitErrorRecorded {
						evaluationLimitErrorRecorded = true
						errorsSeen.add(fmt.Errorf(
							"automation evaluation inventory exceeds the per-run bound of %d",
							maxAutomationShadowEvaluationsPerRun,
						))
					}
					break
				}
				outcomes, observationLayer, err := s.automationShadowRequestOutcomes(
					ctx,
					policy.Scope.ID,
					rule.Trigger.RequestMetric.StatusCodes,
					windowEnd.Add(-window),
					windowEnd,
				)
				if err != nil {
					errorsSeen.add(fmt.Errorf(
						"policy %s rule %s evidence query: %w",
						policy.ID,
						rule.ID,
						err,
					))
					break
				}
				result, err := automation.EvaluatePolicy(automation.EvaluationInput{
					Policy: policy,
					RuleID: rule.ID,
					Evidence: model.AutomationEvaluationEvidence{
						CollectedBy:            model.AutomationIntentSourceControlLoop,
						Trusted:                true,
						WindowStartedAt:        windowEnd.Add(-window),
						WindowEndedAt:          windowEnd,
						RequestOutcomes:        outcomes,
						AppRevision:            appRevision,
						AppReadiness:           readiness,
						AppReadinessObservedAt: readinessObservedAt,
					},
					// A completed window has a stable evaluation time. This
					// makes failover/restart replays deterministic.
					Now: windowEnd,
				})
				if err != nil {
					errorsSeen.add(fmt.Errorf(
						"policy %s rule %s evaluation: %w",
						policy.ID,
						rule.ID,
						err,
					))
					break
				}
				summary.Evaluations++
				if result.Decision.Matched {
					summary.Matches++
				}
				if result.Decision.WouldAction {
					intent, err := automation.NewObservedActionIntent(policy, result)
					if err != nil {
						errorsSeen.add(fmt.Errorf(
							"policy %s rule %s observed intent: %w",
							policy.ID,
							rule.ID,
							err,
						))
						break
					}
					stored, created, err := s.store.CreateAutomationActionIntent(intent)
					if err != nil {
						errorsSeen.add(fmt.Errorf(
							"policy %s rule %s persist observed intent: %w",
							policy.ID,
							rule.ID,
							err,
						))
						break
					}
					if created {
						summary.IntentsCreated++
						s.appendAutomationControlLoopAudit(
							policy,
							stored,
							observationLayer,
						)
					} else {
						summary.IntentsReused++
					}
					_, dispatchCreated, err := s.prepareAutomationActionDispatch(
						stored,
						observationLayer,
					)
					if err != nil {
						errorsSeen.add(fmt.Errorf(
							"policy %s rule %s prepare action dispatch: %w",
							policy.ID,
							rule.ID,
							err,
						))
						break
					}
					if dispatchCreated {
						summary.DispatchesCreated++
					} else {
						summary.DispatchesReused++
					}
				}
				cursor[cursorKey] = windowEnd
			}
		}
	}
	for key := range cursor {
		if _, active := activeCursorKeys[key]; !active {
			delete(cursor, key)
		}
	}
	summary.Errors = errorsSeen.count
	return summary, nextInterval, errorsSeen.err()
}

func automationShadowPolicyBatch(
	policies []model.AutomationPolicy,
	now time.Time,
	interval time.Duration,
) ([]model.AutomationPolicy, int) {
	if len(policies) <= maxAutomationShadowPoliciesPerRun {
		return policies, 0
	}
	if interval < time.Second {
		interval = time.Second
	}
	slotWidthSeconds := int64(interval / time.Second)
	if slotWidthSeconds < 1 {
		slotWidthSeconds = 1
	}
	slot := now.UTC().Unix() / slotWidthSeconds
	count := int64(len(policies))
	step := int64(maxAutomationShadowPoliciesPerRun) % count
	start := ((slot % count) * step) % count
	if start < 0 {
		start += count
	}

	selected := make([]model.AutomationPolicy, 0, maxAutomationShadowPoliciesPerRun)
	for index := 0; index < maxAutomationShadowPoliciesPerRun; index++ {
		selected = append(selected, policies[(int(start)+index)%len(policies)])
	}
	return selected, len(policies) - len(selected)
}

func automationShadowAppEligible(app model.App) bool {
	if app.Spec.Replicas <= 0 {
		return false
	}
	switch strings.TrimSpace(strings.ToLower(app.Status.Phase)) {
	case "disabled", "deleted", "deleting":
		return false
	default:
		return true
	}
}

func automationShadowRuleWindow(rule model.AutomationRule) (time.Duration, error) {
	if rule.Trigger.Type != model.AutomationTriggerRequestMetric ||
		rule.Trigger.Source != "app_request_outcomes" ||
		rule.Trigger.RequestMetric == nil ||
		strings.TrimSpace(strings.ToLower(rule.Trigger.RequestMetric.Metric)) != "http_status" {
		return 0, errors.New("rule is outside the supported app request-outcome contract")
	}
	window, err := time.ParseDuration(strings.TrimSpace(rule.Trigger.RequestMetric.Window))
	if err != nil || window < time.Second || window > 15*time.Minute {
		return 0, errors.New("request outcome window must be between 1s and 15m")
	}
	return window, nil
}

func automationShadowCursorKey(
	policy model.AutomationPolicy,
	rule model.AutomationRule,
) string {
	return strings.Join([]string{
		strings.TrimSpace(policy.ID),
		strconv.FormatInt(policy.Generation, 10),
		strings.TrimSpace(rule.ID),
	}, "\n")
}

func automationShadowDueWindowEnds(
	last time.Time,
	now time.Time,
	window time.Duration,
) ([]time.Time, int) {
	if window <= 0 {
		return nil, 0
	}
	latest := now.UTC().Add(-automationShadowLoopSettleDelay).Truncate(window)
	if latest.IsZero() || !latest.After(time.Time{}) {
		return nil, 0
	}
	if last.IsZero() {
		return []time.Time{latest}, 0
	}
	last = last.UTC()
	if !latest.After(last) {
		return nil, 0
	}
	due := int(latest.Sub(last) / window)
	if due <= 0 {
		return nil, 0
	}
	skipped := 0
	if due > maxAutomationShadowCatchupWindows {
		skipped = due - maxAutomationShadowCatchupWindows
		due = maxAutomationShadowCatchupWindows
		last = latest.Add(-time.Duration(due) * window)
	}
	out := make([]time.Time, 0, due)
	for index := 1; index <= due; index++ {
		out = append(out, last.Add(time.Duration(index)*window))
	}
	return out, skipped
}

func (s *Server) automationShadowRequestOutcomes(
	ctx context.Context,
	appID string,
	statusCodes []int,
	windowStartedAt time.Time,
	windowEndedAt time.Time,
) ([]model.AutomationRequestOutcomeAggregate, string, error) {
	if s.automationRequestOutcomeQuery != nil {
		return s.automationRequestOutcomeQuery(
			ctx,
			appID,
			append([]int(nil), statusCodes...),
			windowStartedAt,
			windowEndedAt,
		)
	}
	queryText, err := buildAutomationRequestOutcomesQuery(
		appID,
		statusCodes,
		windowStartedAt,
		windowEndedAt,
	)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.queryAppObservabilityClickHouse(ctx, queryText)
	if err != nil {
		return nil, "", err
	}
	return automationRequestOutcomesFromClickHouseRows(rows)
}

func buildAutomationRequestOutcomesQuery(
	appID string,
	statusCodes []int,
	windowStartedAt time.Time,
	windowEndedAt time.Time,
) (string, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" ||
		windowStartedAt.IsZero() ||
		windowEndedAt.IsZero() ||
		!windowEndedAt.After(windowStartedAt) {
		return "", errors.New("automation request outcome query requires an app and a valid window")
	}
	codes := append([]int(nil), statusCodes...)
	sort.Ints(codes)
	normalizedCodes := codes[:0]
	previous := 0
	for _, statusCode := range codes {
		if statusCode < 500 || statusCode > 599 {
			return "", errors.New("automation request outcome query only accepts server-error status codes")
		}
		if len(normalizedCodes) > 0 && statusCode == previous {
			continue
		}
		normalizedCodes = append(normalizedCodes, statusCode)
		previous = statusCode
	}
	if len(normalizedCodes) == 0 || len(normalizedCodes) > 100 {
		return "", errors.New("automation request outcome query requires between 1 and 100 status codes")
	}
	codeValues := make([]string, 0, len(normalizedCodes))
	for _, statusCode := range normalizedCodes {
		codeValues = append(codeValues, strconv.Itoa(statusCode))
	}
	rowLimit := 2 * (maxAutomationShadowOutcomeAggregates + 1)
	return "SELECT " +
		"if(edge_id = '', 'app', 'edge') AS observation_layer, " +
		"status_code, " +
		"if(edge_id = '', runtime_id, edge_id) AS failure_domain, " +
		"uniqExact(if(request_id != '', concat('request:', request_id), concat('trace:', trace_id))) AS request_count " +
		"FROM request_facts WHERE " +
		"app_id = " + quoteClickHouseString(appID) +
		" AND ts >= " + clickHouseDateTime64Literal(windowStartedAt) +
		" AND ts < " + clickHouseDateTime64Literal(windowEndedAt) +
		" AND (request_id != '' OR trace_id != '')" +
		" AND status_code IN (" + strings.Join(codeValues, ",") + ") " +
		"GROUP BY observation_layer, status_code, failure_domain " +
		"ORDER BY observation_layer ASC, status_code ASC, failure_domain ASC " +
		"LIMIT " + strconv.Itoa(rowLimit) + " FORMAT JSONEachRow", nil
}

func automationRequestOutcomesFromClickHouseRows(
	rows []map[string]any,
) ([]model.AutomationRequestOutcomeAggregate, string, error) {
	byLayer := map[string][]model.AutomationRequestOutcomeAggregate{
		"app":  {},
		"edge": {},
	}
	for _, row := range rows {
		layer := strings.TrimSpace(strings.ToLower(fmt.Sprint(row["observation_layer"])))
		if layer != "app" && layer != "edge" {
			return nil, "", fmt.Errorf("unsupported automation observation layer %q", layer)
		}
		statusCode, err := strictAutomationInteger(row["status_code"])
		if err != nil || statusCode < 100 || statusCode > 599 {
			return nil, "", errors.New("automation outcome row has an invalid status code")
		}
		count, err := strictAutomationInteger(row["request_count"])
		if err != nil || count <= 0 {
			return nil, "", errors.New("automation outcome row has an invalid request count")
		}
		failureDomain := strings.TrimSpace(fmt.Sprint(row["failure_domain"]))
		if failureDomain == "<nil>" {
			failureDomain = ""
		}
		if failureDomain != "" {
			failureDomain = layer + ":" + failureDomain
		}
		if len(failureDomain) > 200 {
			return nil, "", errors.New("automation outcome failure domain is too long")
		}
		byLayer[layer] = append(byLayer[layer], model.AutomationRequestOutcomeAggregate{
			StatusCode:    int(statusCode),
			Count:         count,
			FailureDomain: failureDomain,
		})
	}

	layer := "none"
	outcomes := []model.AutomationRequestOutcomeAggregate{}
	if len(byLayer["app"]) > 0 {
		layer = "app"
		outcomes = byLayer["app"]
	} else if len(byLayer["edge"]) > 0 {
		layer = "edge"
		outcomes = byLayer["edge"]
	}
	if len(outcomes) > maxAutomationShadowOutcomeAggregates {
		return nil, "", fmt.Errorf(
			"automation outcome aggregate bound exceeded: got=%d max=%d",
			len(outcomes),
			maxAutomationShadowOutcomeAggregates,
		)
	}
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].StatusCode != outcomes[j].StatusCode {
			return outcomes[i].StatusCode < outcomes[j].StatusCode
		}
		return outcomes[i].FailureDomain < outcomes[j].FailureDomain
	})
	return outcomes, layer, nil
}

func strictAutomationInteger(value any) (int64, error) {
	switch typed := value.(type) {
	case float64:
		if math.IsNaN(typed) ||
			math.IsInf(typed, 0) ||
			typed != math.Trunc(typed) ||
			typed < -9223372036854775808.0 ||
			typed >= 9223372036854775808.0 {
			return 0, errors.New("non-integral numeric value")
		}
		return int64(typed), nil
	case float32:
		return strictAutomationInteger(float64(typed))
	case json.Number:
		return typed.Int64()
	case int:
		return int64(typed), nil
	case int64:
		return typed, nil
	case int32:
		return int64(typed), nil
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, errors.New("integer overflows int64")
		}
		return int64(typed), nil
	case uint64:
		if typed > math.MaxInt64 {
			return 0, errors.New("integer overflows int64")
		}
		return int64(typed), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer type %T", value)
	}
}

func (s *Server) appendAutomationControlLoopAudit(
	policy model.AutomationPolicy,
	intent model.AutomationActionIntent,
	observationLayer string,
) {
	s.appendAudit(
		model.Principal{
			ActorType: model.ActorTypeSystem,
			ActorID:   automationShadowLoopActorID,
			TenantID:  policy.TenantID,
			ProjectID: policy.ProjectID,
			AppID:     policy.Scope.ID,
		},
		"automation.policy.evaluate_control_loop",
		"automation_policy",
		policy.ID,
		policy.TenantID,
		map[string]string{
			"app_id":                      policy.Scope.ID,
			"project_id":                  policy.ProjectID,
			"policy_generation":           strconv.FormatInt(policy.Generation, 10),
			"rule_id":                     intent.RuleID,
			"mode":                        intent.Mode,
			"matched":                     "true",
			"would_action":                "true",
			"production_mutation_allowed": "false",
			"matching_samples":            strconv.FormatInt(intent.Decision.MatchingSamples, 10),
			"failure_domains":             strconv.Itoa(len(intent.Decision.FailureDomains)),
			"evidence_hash":               intent.EvidenceHash,
			"source":                      model.AutomationIntentSourceControlLoop,
			"observation_layer":           observationLayer,
			"window_started_at":           intent.Evidence.WindowStartedAt.UTC().Format(time.RFC3339Nano),
			"window_ended_at":             intent.Evidence.WindowEndedAt.UTC().Format(time.RFC3339Nano),
			"intent_id":                   intent.ID,
			"idempotency_key":             intent.IdempotencyKey,
			"intent_created":              "true",
		},
	)
}

func (s *Server) setAutomationShadowLoopLeader(leader bool) {
	s.automationShadowLoop.mu.Lock()
	s.automationShadowLoop.leader = leader
	s.automationShadowLoop.mu.Unlock()
}

func (s *Server) recordAutomationShadowLoopContention() {
	s.automationShadowLoop.mu.Lock()
	s.automationShadowLoop.leadershipContentionCount++
	s.automationShadowLoop.mu.Unlock()
}

func (s *Server) recordAutomationShadowLoopLeadershipError(err error) {
	if err == nil {
		return
	}
	s.automationShadowLoop.mu.Lock()
	s.automationShadowLoop.errorCount++
	s.automationShadowLoop.lastError = err.Error()
	s.automationShadowLoop.mu.Unlock()
}

func (s *Server) recordAutomationShadowLoopRun(
	started time.Time,
	duration time.Duration,
	summary automationShadowLoopRunSummary,
	err error,
) {
	s.automationShadowLoop.mu.Lock()
	defer s.automationShadowLoop.mu.Unlock()
	s.automationShadowLoop.lastRun = started.UTC()
	s.automationShadowLoop.lastDuration = duration
	s.automationShadowLoop.lastSummary = summary
	s.automationShadowLoop.runCount++
	s.automationShadowLoop.policiesScannedCount += int64(summary.PoliciesScanned)
	s.automationShadowLoop.evaluationCount += int64(summary.Evaluations)
	s.automationShadowLoop.matchCount += int64(summary.Matches)
	s.automationShadowLoop.intentCreatedCount += int64(summary.IntentsCreated)
	s.automationShadowLoop.intentReusedCount += int64(summary.IntentsReused)
	s.automationShadowLoop.dispatchCreatedCount += int64(summary.DispatchesCreated)
	s.automationShadowLoop.dispatchReusedCount += int64(summary.DispatchesReused)
	s.automationShadowLoop.ineligiblePolicyCount += int64(summary.IneligiblePolicies)
	s.automationShadowLoop.policyLimitDeferredCount += int64(summary.PolicyLimitDeferred)
	s.automationShadowLoop.catchupWindowSkippedCount += int64(summary.CatchupWindowsSkipped)
	s.automationShadowLoop.evaluationLimitSkippedCount += int64(summary.EvaluationLimitSkipped)
	if err != nil {
		s.automationShadowLoop.errorCount++
		s.automationShadowLoop.lastError = err.Error()
		return
	}
	s.automationShadowLoop.lastSuccess = time.Now().UTC()
	s.automationShadowLoop.lastError = ""
}

func (s *Server) writeAutomationShadowLoopMetrics(w io.Writer) {
	s.automationShadowLoop.mu.Lock()
	leader := s.automationShadowLoop.leader
	lastRun := s.automationShadowLoop.lastRun
	lastSuccess := s.automationShadowLoop.lastSuccess
	lastDuration := s.automationShadowLoop.lastDuration
	lastSummary := s.automationShadowLoop.lastSummary
	lastError := s.automationShadowLoop.lastError
	runCount := s.automationShadowLoop.runCount
	errorCount := s.automationShadowLoop.errorCount
	leadershipContentionCount := s.automationShadowLoop.leadershipContentionCount
	policiesScannedCount := s.automationShadowLoop.policiesScannedCount
	evaluationCount := s.automationShadowLoop.evaluationCount
	matchCount := s.automationShadowLoop.matchCount
	intentCreatedCount := s.automationShadowLoop.intentCreatedCount
	intentReusedCount := s.automationShadowLoop.intentReusedCount
	dispatchCreatedCount := s.automationShadowLoop.dispatchCreatedCount
	dispatchReusedCount := s.automationShadowLoop.dispatchReusedCount
	ineligiblePolicyCount := s.automationShadowLoop.ineligiblePolicyCount
	policyLimitDeferredCount := s.automationShadowLoop.policyLimitDeferredCount
	catchupWindowSkippedCount := s.automationShadowLoop.catchupWindowSkippedCount
	evaluationLimitSkippedCount := s.automationShadowLoop.evaluationLimitSkippedCount
	s.automationShadowLoop.mu.Unlock()

	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_enabled", "Whether the observe-only automation loop is configured.", nil, boolMetric(s.automationShadowLoopConfig.Enabled))
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_active", "Whether the observe-only automation loop has its required analytics backend.", nil, boolMetric(s.automationShadowLoopActive()))
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_leader", "Whether this API replica currently leads the observe-only automation loop.", nil, boolMetric(leader))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_runs_total", "Total observe-only automation loop runs.", nil, float64(runCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_errors_total", "Total observe-only automation loop or leader-election errors.", nil, float64(errorCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_leadership_contention_total", "Total failed attempts to acquire observe-only automation loop leadership.", nil, float64(leadershipContentionCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_policies_scanned_total", "Total policies scanned by the observe-only automation loop.", nil, float64(policiesScannedCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_evaluations_total", "Total trusted policy-window evaluations.", nil, float64(evaluationCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_matches_total", "Total trusted policy-window matches.", nil, float64(matchCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_intents_created_total", "Total append-only observe intents created by the control loop.", nil, float64(intentCreatedCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_intents_reused_total", "Total idempotent observe intents reused by the control loop.", nil, float64(intentReusedCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_dispatches_created_total", "Total durable action WAL entries created by the control loop.", nil, float64(dispatchCreatedCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_dispatches_reused_total", "Total durable action WAL entries reused by the control loop.", nil, float64(dispatchReusedCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_ineligible_policies_total", "Total disabled-app policy windows skipped without querying telemetry.", nil, float64(ineligiblePolicyCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_policy_limit_deferred_total", "Total shadow policies deferred to a later rotating batch by the per-run inventory bound.", nil, float64(policyLimitDeferredCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_catchup_windows_skipped_total", "Total old shadow windows skipped by the bounded catch-up policy.", nil, float64(catchupWindowSkippedCount))
	observability.WriteCounterMetric(w, "fugue_automation_shadow_loop_evaluation_limit_skipped_total", "Total evaluations skipped by the per-run safety bound.", nil, float64(evaluationLimitSkippedCount))
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_duration_seconds", "Duration of the last observe-only automation loop run.", nil, lastDuration.Seconds())
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_policies", "Policies scanned by the last observe-only automation loop run.", nil, float64(lastSummary.PoliciesScanned))
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_policies_selected", "Shadow policies selected by the rotating per-run batch.", nil, float64(lastSummary.PoliciesSelected))
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_evaluations", "Policy windows evaluated by the last observe-only automation loop run.", nil, float64(lastSummary.Evaluations))
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_matches", "Policy windows matched by the last observe-only automation loop run.", nil, float64(lastSummary.Matches))
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_intents_created", "Observe intents created by the last automation loop run.", nil, float64(lastSummary.IntentsCreated))
	if !lastRun.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_run_timestamp_seconds", "Unix timestamp of the last observe-only automation loop run.", nil, float64(lastRun.Unix()))
	}
	if !lastSuccess.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_success_timestamp_seconds", "Unix timestamp of the last fully successful observe-only automation loop run.", nil, float64(lastSuccess.Unix()))
	}
	observability.WriteGaugeMetric(w, "fugue_automation_shadow_loop_last_error", "Whether the last observe-only automation loop run failed.", map[string]string{"error": truncateMetricLabel(lastError, 160)}, boolMetric(lastError != ""))
}
