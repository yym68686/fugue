package observability

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	edgeRouteDecisionLinkCheckInterval = 30 * time.Second
	edgeRouteDecisionLinkGrace         = 2 * time.Minute
	EdgeRouteDecisionRetention         = 30 * 24 * time.Hour
	edgeRouteDecisionLinkLimit         = 100
)

func (p *Pipeline) runEdgeRouteDecisionLinkChecks() {
	defer p.wg.Done()
	ticker := time.NewTicker(edgeRouteDecisionLinkCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkEdgeRouteDecisionLinks(p.ctx)
		}
	}
}

func (p *Pipeline) checkEdgeRouteDecisionLinks(ctx context.Context) {
	if p == nil || strings.TrimSpace(p.cfg.ClickHouseDSN) == "" {
		return
	}
	p.edgeRouteLinkChecks.Add(1)
	client := NewClickHouseExporter(p.cfg.ClickHouseDSN, p.httpClient)
	query := edgeRouteDecisionMissingLinkQuery(client.Target.Database)
	queryCtx, cancel := context.WithTimeout(ctx, p.cfg.ExportTimeout)
	defer cancel()
	rows, err := client.QueryJSONEachRow(queryCtx, query, p.cfg.ClickHouseQueryMaxPayloadBytes)
	if err != nil {
		p.edgeRouteLinkErrors.Add(1)
		p.recordError(fmt.Errorf("check edge route decision links: %w", err))
		return
	}
	p.edgeRouteMissingLinks.Store(int64(len(rows)))
	for _, row := range rows {
		event, ok := edgeRouteDecisionMissingLinkEvent(row)
		if !ok {
			p.edgeRouteLinkErrors.Add(1)
			p.recordError(fmt.Errorf("check edge route decision links: invalid row"))
			continue
		}
		if p.Ingest(ctx, event) {
			p.edgeRouteLinkAlerts.Add(1)
		}
	}
}

func edgeRouteDecisionMissingLinkQuery(database string) string {
	requestFacts := clickHouseQualifiedTable(database, "request_facts")
	decisions := clickHouseQualifiedTable(database, "edge_route_decisions")
	alerts := clickHouseQualifiedTable(database, "edge_route_decision_missing_links")
	return fmt.Sprintf(`SELECT DISTINCT
  rf.ts AS ts,
  rf.tenant_id AS tenant_id,
  rf.app_id AS app_id,
  rf.hostname AS hostname,
  rf.request_id AS request_id,
  rf.edge_id AS edge_id,
  rf.decision_id AS decision_id,
  rf.bundle_version AS bundle_version,
  rf.route_generation AS route_generation,
  rf.correlation_key AS correlation_key,
  rf.status_reason AS status_reason,
  rf.platform_error_class AS platform_error_class
FROM %s AS rf
LEFT ANTI JOIN (SELECT tenant_id, app_id, decision_id, bundle_version, route_generation FROM %s FINAL) AS decision
  ON decision.tenant_id = rf.tenant_id
 AND decision.app_id = rf.app_id
 AND decision.decision_id = rf.decision_id
 AND decision.bundle_version = rf.bundle_version
 AND decision.route_generation = rf.route_generation
LEFT ANTI JOIN (SELECT tenant_id, app_id, correlation_key, request_id FROM %s FINAL) AS prior_alert
  ON prior_alert.tenant_id = rf.tenant_id
 AND prior_alert.app_id = rf.app_id
 AND prior_alert.correlation_key = rf.correlation_key
 AND prior_alert.request_id = rf.request_id
WHERE rf.ts >= now64(3) - INTERVAL 30 DAY
  AND rf.ts <= now64(3) - INTERVAL 2 MINUTE
  AND rf.status_code >= 500
  AND rf.platform_error_class IN ('route_unavailable', 'no_healthy', 'bundle_signature', 'invariant')
  AND rf.decision_id != ''
  AND rf.bundle_version != ''
  AND rf.route_generation != ''
  AND rf.correlation_key != ''
ORDER BY rf.ts ASC, rf.request_id ASC
LIMIT %d
FORMAT JSONEachRow`, requestFacts, decisions, alerts, edgeRouteDecisionLinkLimit)
}

func edgeRouteDecisionMissingLinkEvent(row map[string]any) (Event, bool) {
	attrs := map[string]string{
		"event_type":           "edge_route_decision_material_missing",
		"severity":             "page",
		"tenant_id":            rowString(row, "tenant_id"),
		"app_id":               rowString(row, "app_id"),
		"hostname":             rowString(row, "hostname"),
		"request_id":           rowString(row, "request_id"),
		"edge_id":              rowString(row, "edge_id"),
		"decision_id":          rowString(row, "decision_id"),
		"bundle_version":       rowString(row, "bundle_version"),
		"route_generation":     rowString(row, "route_generation"),
		"correlation_key":      rowString(row, "correlation_key"),
		"status_reason":        rowString(row, "status_reason"),
		"platform_error_class": "decision_missing",
		"reason":               "decision material absent after 2m grace period",
	}
	for _, key := range []string{"app_id", "request_id", "decision_id", "bundle_version", "route_generation", "correlation_key"} {
		if attrs[key] == "" {
			return Event{}, false
		}
	}
	timestamp, err := time.Parse("2006-01-02 15:04:05.999", rowString(row, "ts"))
	if err != nil {
		timestamp, err = time.Parse(time.RFC3339Nano, rowString(row, "ts"))
		if err != nil {
			return Event{}, false
		}
	}
	return Event{
		Timestamp:  timestamp.UTC(),
		Kind:       EventKindLog,
		Source:     "clickhouse://edge-route-decision-link-check",
		Message:    "decision-required edge request is missing persisted route decision material",
		Attributes: attrs,
	}, true
}

func rowString(row map[string]any, key string) string {
	value := row[key]
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}
