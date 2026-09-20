package livediagnostics

import (
	"encoding/json"
	"errors"

	batchv1 "k8s.io/api/batch/v1"
)

// ValidateProbeReport checks the stable envelope while allowing future packs to
// add fields. Evidence data itself belongs to the independently versioned pack.
func ValidateProbeReport(value any, job batchv1.Job) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var report ProbeReport
	if err := json.Unmarshal(raw, &report); err != nil {
		return err
	}
	if report.ProbeImage != JobRunnerImage(job) {
		return errors.New("probe report image does not match the admitted package")
	}
	if report.Schema != "fugue.diagnostic.probe_report.v1" || report.SessionID != job.Name || report.ProbeID != job.Annotations[ProbeRefAnnotation] || report.ProbeDigest != job.Annotations[ProbeDigestAnnotation] || report.CatalogDigest != job.Annotations[CatalogDigestAnnotation] {
		return errors.New("probe report identity does not match the admitted session")
	}
	if report.Target.Type != TargetType(job.Labels[TargetTypeLabel]) || report.Target.Node != job.Annotations[TargetNodeAnnotation] || report.Target.PodUID != job.Annotations[TargetPodUIDAnnotation] || report.Target.Namespace != job.Annotations[TargetNamespaceAnnotation] || report.Target.Pod != job.Annotations[TargetPodAnnotation] || report.Target.Container != job.Annotations[TargetContainerAnnotation] || report.Target.ProcessName != job.Annotations[TargetProcessAnnotation] {
		return errors.New("probe report target does not match the frozen runtime identity")
	}
	if report.StartedAt.IsZero() || report.FinishedAt.Before(report.StartedAt) {
		return errors.New("probe report has no valid observation window")
	}
	switch report.Quality.Status {
	case "complete", "degraded", "unavailable":
	default:
		return errors.New("probe report has no evidence quality status")
	}
	if report.Quality.Status == "complete" {
		if len(report.Evidence) == 0 || len(report.Quality.Gaps) > 0 || report.Quality.Truncated {
			return errors.New("incomplete evidence cannot be reported as complete")
		}
		for _, e := range report.Evidence {
			if e.Status != "complete" {
				return errors.New("incomplete source cannot be reported as complete")
			}
		}
	}
	return nil
}
