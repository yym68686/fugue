package walreplay

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

type Incident struct {
	ID          string            `json:"id"`
	Component   string            `json:"component"`
	Action      string            `json:"action"`
	Subject     string            `json:"subject,omitempty"`
	Generation  string            `json:"generation,omitempty"`
	RecordCount int               `json:"record_count"`
	FirstSeenAt time.Time         `json:"first_seen_at"`
	LastSeenAt  time.Time         `json:"last_seen_at"`
	Evidence    map[string]string `json:"evidence,omitempty"`
}

type Result struct {
	Incidents []Incident                  `json:"incidents"`
	Summary   model.LocalWALReplaySummary `json:"summary"`
}

func Replay(paths []string, now time.Time) (Result, error) {
	return ReplayWithSignerSecrets(paths, now, nil)
}

func ReplayWithSignerSecrets(paths []string, now time.Time, signerSecrets map[string][]byte) (Result, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result := Result{Summary: model.LocalWALReplaySummary{ReplayedAt: now.UTC()}}
	incidentByKey := map[string]*Incident{}
	for _, path := range paths {
		records, err := localwal.ReadAll(path)
		if err != nil {
			result.Summary.RecordsRejected++
			return result, err
		}
		result.Summary.RecordsRead += len(records)
		for _, record := range records {
			if len(signerSecrets) > 0 {
				secret := signerSecrets[strings.TrimSpace(record.Signer)]
				if err := localwal.VerifyRecord(record, secret); err != nil {
					result.Summary.RecordsRejected++
					continue
				}
			}
			if record.ExpiresAt != nil && !now.Before(*record.ExpiresAt) {
				result.Summary.RecordsExpired++
				if isTemporaryAction(record.Action) {
					result.Summary.TemporaryActionsGC++
				}
				continue
			}
			if record.Component == "" || record.Action == "" {
				result.Summary.RecordsRejected++
				continue
			}
			result.Summary.RecordsAccepted++
			key := incidentKey(record)
			incident := incidentByKey[key]
			if incident == nil {
				incident = &Incident{
					ID:          model.NewID("incident"),
					Component:   strings.TrimSpace(record.Component),
					Action:      strings.TrimSpace(record.Action),
					Subject:     strings.TrimSpace(record.Subject),
					Generation:  strings.TrimSpace(record.Generation),
					FirstSeenAt: record.RecordedAt,
					LastSeenAt:  record.RecordedAt,
					Evidence:    map[string]string{},
				}
				incidentByKey[key] = incident
			}
			incident.RecordCount++
			if record.RecordedAt.Before(incident.FirstSeenAt) {
				incident.FirstSeenAt = record.RecordedAt
			}
			if record.RecordedAt.After(incident.LastSeenAt) {
				incident.LastSeenAt = record.RecordedAt
			}
			for k, v := range record.Evidence {
				if strings.TrimSpace(k) != "" && incident.Evidence[strings.TrimSpace(k)] == "" {
					incident.Evidence[strings.TrimSpace(k)] = strings.TrimSpace(v)
				}
			}
		}
	}
	keys := make([]string, 0, len(incidentByKey))
	for key := range incidentByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		incident := *incidentByKey[key]
		if len(incident.Evidence) == 0 {
			incident.Evidence = nil
		}
		result.Incidents = append(result.Incidents, incident)
	}
	result.Summary.IncidentsMerged = len(result.Incidents)
	if len(result.Incidents) > 0 {
		result.Summary.Evidence = map[string]string{"incident_summary": fmt.Sprintf("%d merged incident(s)", len(result.Incidents))}
	}
	return result, nil
}

func incidentKey(record localwal.Record) string {
	return strings.Join([]string{
		strings.TrimSpace(record.Component),
		strings.TrimSpace(record.Action),
		strings.TrimSpace(record.Subject),
		strings.TrimSpace(record.Generation),
	}, "|")
}

func isTemporaryAction(action string) bool {
	action = strings.TrimSpace(action)
	return strings.Contains(action, "temporary") || strings.Contains(action, "fallback") || strings.Contains(action, "filter")
}
