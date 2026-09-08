package cli

import (
	"errors"
	"time"
)

type evidenceSource struct {
	State      string    `json:"state"`
	ObservedAt time.Time `json:"observed_at"`
	ErrorCode  string    `json:"error_code,omitempty"`
	Message    string    `json:"message,omitempty"`
}

func (s *appOverviewSnapshot) recordSource(name string, err error, empty bool) {
	if s.Sources == nil {
		s.Sources = map[string]evidenceSource{}
	}
	src := makeEvidenceSource(err, empty)
	if err != nil {
		s.MissingEvidence = append(s.MissingEvidence, name)
		s.Completeness = "partial"
	}
	if s.Completeness == "" {
		s.Completeness = "complete"
	}
	s.Sources[name] = src
}

var errEvidenceUnavailable = errors.New("source did not provide evidence")

func makeEvidenceSource(err error, empty bool) evidenceSource {
	src := evidenceSource{State: "available", ObservedAt: time.Now().UTC()}
	if empty {
		src.State = "empty"
	}
	if err != nil {
		src.State = "unavailable"
		switch ExitCodeForError(err) {
		case ExitCodePermissionDenied:
			src.State = "permission_denied"
		case ExitCodeNotFound:
			src.State = "unavailable"
		}
		src.ErrorCode = describeCommandError(err).Code
		src.Message = redactDiagnosticString(err.Error())
	}
	return src
}
