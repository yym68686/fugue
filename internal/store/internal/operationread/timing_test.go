package operationread

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestFirstDeployExecutionStartsUsesOnlyAuthorizedRunningDeploySteps(t *testing.T) {
	now := time.Unix(100, 0).In(time.FixedZone("offset", 3600))
	first := now.Add(time.Second)
	read := func(visit func([]model.ReleaseStep)) error {
		visit([]model.ReleaseStep{
			{OperationID: "deploy", Type: model.ReleaseStepTypeDeployApply, Status: model.ReleaseStepStatusRunning, StartedAt: now.Add(2 * time.Second)},
			{OperationID: "deploy", Type: model.ReleaseStepTypeDeployApply, Status: model.ReleaseStepStatusRunning, StartedAt: first},
			{OperationID: "deploy", Type: "plan", Status: model.ReleaseStepStatusRunning, StartedAt: now},
			{OperationID: "deploy", Type: model.ReleaseStepTypeDeployApply, Status: "completed", StartedAt: now},
			{OperationID: "unrelated", Type: model.ReleaseStepTypeDeployApply, Status: model.ReleaseStepStatusRunning, StartedAt: now},
		})
		return nil
	}
	got, err := FirstDeployExecutionStarts(t.Context(), nil, read, []string{"deploy"})
	if err != nil || len(got) != 1 || got["deploy"] != first.UTC() {
		t.Fatalf("got %v, err %v", got, err)
	}
}

func TestFirstDeployExecutionStartsShortCircuitsAndRetainsReadErrors(t *testing.T) {
	if got, err := FirstDeployExecutionStarts(t.Context(), nil, nil, nil); err != nil || len(got) != 0 {
		t.Fatalf("zero input got %v, err %v", got, err)
	}
	readErr := errors.New("unavailable")
	if _, err := FirstDeployExecutionStarts(t.Context(), nil, func(func([]model.ReleaseStep)) error {
		return readErr
	}, []string{"deploy"}); !errors.Is(err, readErr) {
		t.Fatalf("read error was replaced: %v", err)
	}
}
