package releaseflow

import (
	"fmt"
	"strings"
)

type RolloutReadinessResult struct {
	Ready              bool
	Phase              string
	AppID              string
	OperationID        string
	ExpectedReleaseKey string
	CurrentReleaseKey  string
	EvidenceID         string
	SchedulingReason   string
	PodFailureReason   string
	Message            string
	Err                error
}

func (r RolloutReadinessResult) Error() error {
	if r.Err != nil {
		return r.Err
	}
	if r.Ready {
		return nil
	}
	message := strings.TrimSpace(r.Message)
	if message == "" {
		message = strings.TrimSpace(r.Phase)
	}
	if message == "" {
		message = "rollout did not become ready"
	}
	return fmt.Errorf("%s", message)
}
