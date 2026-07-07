package releaseflow

import (
	"time"

	"fugue/internal/model"
)

type ReleaseEvidenceView struct {
	Metadata            map[string]any
	Operation           *model.Operation
	App                 *model.App
	ImageTracking       *model.AppImageTracking
	ImageTrackingChecks []model.AppImageTrackingCheck
	MetricsSummary      map[string]any
	Diagnosis           *model.OperationDiagnosis
	Timeline            []model.OperationTimelineEntry
	Evidence            []model.OperationEvidence
	ReleaseAttempt      *model.ReleaseAttempt
	ReleaseTimeline     []model.ReleaseTimelineEntry
	AppReleases         []model.AppRelease
	TrafficPolicies     []model.AppTrafficPolicy
	GateResults         []model.AppReleaseGateResult
	DrainMetricsSummary map[string]any
}

type ReleaseEvidenceViewBuilder struct {
	Now func() time.Time
}

func (b ReleaseEvidenceViewBuilder) OperationBundle(view ReleaseEvidenceView) model.OperationDebugBundle {
	metadata := b.metadata(view.Metadata, "operation_debug_bundle")
	var operation model.Operation
	if view.Operation != nil {
		operation = *view.Operation
	}
	return model.OperationDebugBundle{
		Metadata:            metadata,
		Operation:           operation,
		App:                 cloneAppPointer(view.App),
		ImageTracking:       cloneAppImageTrackingPointer(view.ImageTracking),
		ImageTrackingChecks: append([]model.AppImageTrackingCheck(nil), view.ImageTrackingChecks...),
		MetricsSummary:      cloneMap(view.MetricsSummary),
		Diagnosis:           cloneOperationDiagnosisPointer(view.Diagnosis),
		Timeline:            append([]model.OperationTimelineEntry(nil), view.Timeline...),
		Evidence:            append([]model.OperationEvidence(nil), view.Evidence...),
		ReleaseAttempt:      cloneReleaseAttemptPointer(view.ReleaseAttempt),
		ReleaseTimeline:     append([]model.ReleaseTimelineEntry(nil), view.ReleaseTimeline...),
		AppReleases:         append([]model.AppRelease(nil), view.AppReleases...),
		TrafficPolicies:     append([]model.AppTrafficPolicy(nil), view.TrafficPolicies...),
		GateResults:         append([]model.AppReleaseGateResult(nil), view.GateResults...),
		DrainMetricsSummary: cloneMap(view.DrainMetricsSummary),
		RedactionReport:     defaultReleaseEvidenceRedactionReport("operation evidence payloads are stored after server-side redaction"),
	}
}

func (b ReleaseEvidenceViewBuilder) ReleaseBundle(view ReleaseEvidenceView) model.ReleaseDebugBundle {
	metadata := b.metadata(view.Metadata, "release_debug_bundle")
	var attempt model.ReleaseAttempt
	if view.ReleaseAttempt != nil {
		attempt = *view.ReleaseAttempt
	}
	return model.ReleaseDebugBundle{
		Metadata:            metadata,
		ReleaseAttempt:      attempt,
		App:                 cloneAppPointer(view.App),
		ImageTracking:       cloneAppImageTrackingPointer(view.ImageTracking),
		ImageTrackingChecks: append([]model.AppImageTrackingCheck(nil), view.ImageTrackingChecks...),
		MetricsSummary:      cloneMap(view.MetricsSummary),
		ReleaseTimeline:     append([]model.ReleaseTimelineEntry(nil), view.ReleaseTimeline...),
		Evidence:            append([]model.OperationEvidence(nil), view.Evidence...),
		AppReleases:         append([]model.AppRelease(nil), view.AppReleases...),
		TrafficPolicies:     append([]model.AppTrafficPolicy(nil), view.TrafficPolicies...),
		GateResults:         append([]model.AppReleaseGateResult(nil), view.GateResults...),
		DrainMetricsSummary: cloneMap(view.DrainMetricsSummary),
		RedactionReport:     defaultReleaseEvidenceRedactionReport("release evidence payloads are stored after server-side redaction"),
	}
}

func (b ReleaseEvidenceViewBuilder) metadata(input map[string]any, kind string) map[string]any {
	metadata := cloneMap(input)
	if metadata == nil {
		metadata = map[string]any{}
	}
	if _, ok := metadata["generated_at"]; !ok {
		now := time.Now().UTC()
		if b.Now != nil {
			now = b.Now().UTC()
		}
		metadata["generated_at"] = now.Format(time.RFC3339)
	}
	if _, ok := metadata["kind"]; !ok {
		metadata["kind"] = kind
	}
	if _, ok := metadata["redacted"]; !ok {
		metadata["redacted"] = true
	}
	return metadata
}

func GateResultsFromReleaseTimeline(timeline []model.ReleaseTimelineEntry) []model.AppReleaseGateResult {
	results := []model.AppReleaseGateResult{}
	for _, entry := range timeline {
		gateRaw, ok := entry.Payload["gate"]
		if !ok {
			continue
		}
		if gate, ok := gateRaw.(model.AppReleaseGateResult); ok {
			results = append(results, gate)
		}
	}
	return results
}

func defaultReleaseEvidenceRedactionReport(note string) []map[string]any {
	return []map[string]any{{
		"status": "redacted",
		"note":   note,
	}}
}

func cloneMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneAppPointer(input *model.App) *model.App {
	if input == nil {
		return nil
	}
	out := *input
	return &out
}

func cloneAppImageTrackingPointer(input *model.AppImageTracking) *model.AppImageTracking {
	if input == nil {
		return nil
	}
	out := *input
	return &out
}

func cloneOperationDiagnosisPointer(input *model.OperationDiagnosis) *model.OperationDiagnosis {
	if input == nil {
		return nil
	}
	out := *input
	return &out
}

func cloneReleaseAttemptPointer(input *model.ReleaseAttempt) *model.ReleaseAttempt {
	if input == nil {
		return nil
	}
	out := *input
	return &out
}
