package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

type evidenceRelation struct {
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Source  string `json:"source"`
	Command string `json:"command,omitempty"`
}
type objectEvidenceReport struct {
	SchemaVersion   string                    `json:"schema_version"`
	Kind            string                    `json:"kind"`
	ID              string                    `json:"id"`
	Facts           map[string]any            `json:"facts"`
	Sources         map[string]evidenceSource `json:"sources"`
	Completeness    string                    `json:"completeness"`
	MissingEvidence []string                  `json:"missing_evidence"`
	Relations       []evidenceRelation        `json:"relations"`
}

func newObjectEvidenceReport(kind, id string) objectEvidenceReport {
	return objectEvidenceReport{SchemaVersion: "fugue.evidence.v1", Kind: kind, ID: id, Facts: map[string]any{}, Sources: map[string]evidenceSource{}, Completeness: "complete", MissingEvidence: []string{}, Relations: []evidenceRelation{}}
}
func (r *objectEvidenceReport) add(name string, value any, err error, empty bool) {
	source := makeEvidenceSource(err, empty)
	r.Sources[name] = source
	if err != nil {
		r.Completeness = "partial"
		r.MissingEvidence = appendUniqueString(r.MissingEvidence, name)
	} else {
		r.Facts[name] = value
	}
}
func (r *objectEvidenceReport) link(kind, id, source, command string) {
	if id == "" {
		return
	}
	for _, link := range r.Relations {
		if link.Kind == kind && link.ID == id {
			return
		}
	}
	r.Relations = append(r.Relations, evidenceRelation{kind, id, source, command})
}
func (c *CLI) newObjectEvidenceCommand(kind string) *cobra.Command {
	var requireComplete bool
	var timeout time.Duration
	var since string
	use := kind + " <id>"
	args := cobra.ExactArgs(1)
	if kind == "trace" {
		use = "trace <app> <trace-id>"
		args = cobra.ExactArgs(2)
	}
	cmd := &cobra.Command{Use: use, Short: "Collect typed " + kind + " evidence with source availability and exact relations", Args: args, RunE: func(cmd *cobra.Command, args []string) error {
		if timeout <= 0 {
			return fmt.Errorf("timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		scoped := *client
		scoped.context = ctx
		report := newObjectEvidenceReport(kind, args[len(args)-1])
		var primaryErr error
		switch kind {
		case "operation":
			op, e := scoped.GetOperation(args[0])
			primaryErr = e
			report.add("operation", op, e, false)
			if e == nil {
				report.link("app", op.AppID, "operation.app_id", "fugue app overview "+op.AppID)
				d, e := scoped.GetOperationDiagnosis(op.ID)
				report.add("diagnosis", d, e, false)
				if e == nil {
					for _, blocker := range d.BlockedBy {
						report.link("operation", blocker.OperationID, "diagnosis.blocked_by", "fugue diagnose operation "+blocker.OperationID)
					}
					for _, missing := range d.MissingEvidence {
						report.MissingEvidence = appendUniqueString(report.MissingEvidence, "diagnosis."+missing)
						report.Completeness = "partial"
					}
				}
				ev, e := scoped.GetOperationEvidence(op.ID, false)
				report.add("evidence", ev, e, len(ev) == 0)
				if e == nil {
					for _, item := range ev {
						report.link("release_attempt", item.ReleaseAttemptID, "evidence.release_attempt_id", "")
					}
				}
				timeline, e := scoped.GetOperationTimeline(op.ID, false)
				report.add("timeline", timeline, e, len(timeline) == 0)
			}
		case "request":
			v, e := scoped.ExplainRequest(args[0], since)
			primaryErr = e
			report.add("request", v, e, !v.Found)
			if e == nil && v.Found {
				// These are structured server attributes, never guessed from log text.
				for _, key := range []string{"operation_id", "trace_id", "app_id"} {
					if id := v.Evidence[key]; id != "" {
						kind := strings.TrimSuffix(key, "_id")
						report.link(kind, id, "request.evidence."+key, "")
					}
				}
			}
		case "trace":
			app, e := c.resolveWorkspaceApp(&scoped, args[0])
			if e != nil {
				primaryErr = e
				report.add("app", nil, e, false)
				break
			}
			report.link("app", app.ID, "explicit_app_argument", "fugue app overview "+app.ID)
			v, e := scoped.GetAppObservabilityTrace(app.ID, args[1])
			primaryErr = e
			if e == nil && !v.Source.Available {
				e = errEvidenceUnavailable
			}
			report.add("trace", v, e, len(v.Spans) == 0)
			if e == nil && strings.EqualFold(v.Source.Freshness, "stale") {
				source := report.Sources["trace"]
				source.State = "stale"
				report.Sources["trace"] = source
				report.Completeness = "partial"
				report.MissingEvidence = append(report.MissingEvidence, "fresh_trace")
			}
		case "incident":
			incident, status, e := scoped.GetRobustnessIncident(args[0], "")
			primaryErr = e
			report.add("incident", incident, e, false)
			if e == nil {
				report.add("platform_status", status, nil, false)
				for _, key := range []string{"operation_id", "request_id", "app_id"} {
					report.link(strings.TrimSuffix(key, "_id"), incident.Evidence[key], "incident.evidence."+key, "")
				}
			}
		}
		// All adapters use the same diagnostic redaction and JSON number handling.
		raw, e := json.Marshal(report)
		if e != nil {
			return e
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var payload any
		if e = decoder.Decode(&payload); e != nil {
			return e
		}
		payload = redactDiagnosticJSONValue(payload, false)
		if e = c.renderResourceResult(payload); e != nil {
			return e
		}
		if primaryErr != nil {
			return primaryErr
		}
		if requireComplete && report.Completeness != "complete" {
			return withExitCode(errEvidenceUnavailable, ExitCodeIndeterminate)
		}
		return nil
	}}
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Total evidence collection deadline")
	cmd.Flags().BoolVar(&requireComplete, "require-complete", false, "Fail if any requested source or reported dependency evidence is missing")
	if kind == "request" {
		cmd.Flags().StringVar(&since, "since", "1h", "Request evidence lookback window")
	}
	return cmd
}
