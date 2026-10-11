package managedssh

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRuntimeDiagnosticNeverExposesUnderlyingOutput(test *testing.T) {
	cause := errors.New("private runtime command output")
	for _, code := range []string{"deadline", "exit_7", "empty", "unavailable"} {
		err := RuntimeCheckFailure("capability_probe", code, cause)
		if strings.Contains(err.Error(), cause.Error()) || !errors.Is(err, cause) || RuntimeFailureReason(fmt.Errorf("private wrapper: %w", err)) != "capability_probe:"+code {
			test.Fatal("diagnostic classification or privacy failed")
		}
	}
	err := RuntimeCheckFailure(cause.Error(), cause.Error(), cause)
	if RuntimeFailureReason(err) != "unknown:unknown" || strings.Contains(err.Error(), "private") || RuntimeFailureReason(cause) != "unclassified" {
		test.Fatal("untrusted diagnostic labels reached the audit reason")
	}
}

type failingValidationExecutor struct {
	testExecutor
	checks atomic.Int32
}

func (executor *failingValidationExecutor) Validate(ctx context.Context, target Target) (Capabilities, error) {
	if executor.checks.Add(1) > 1 {
		return Capabilities{}, RuntimeCheckFailure("capability_output", "empty", errors.New("private runtime details"))
	}
	return executor.testExecutor.Validate(ctx, target)
}

func TestSessionRejectionAuditsBoundRuntimeReason(test *testing.T) {
	configuration, hostSigner, userSigner := testConfiguration(test)
	audit := &testAudit{}
	_, address := serveForTest(test, &failingValidationExecutor{}, audit, configuration, hostSigner)
	client := connectForTest(test, address, "app", hostSigner, userSigner)
	session, err := client.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	defer session.Close()
	if err := session.Run("sensitive-user-command"); err == nil {
		test.Fatal("failed capability check admitted execution")
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	found := false
	for _, event := range audit.events {
		if event.Type == "session_request_rejected" && event.Reason == "capability_output:empty" && event.TargetPodUID == configuration.Target.PodUID && event.ConnectionID != "" && event.SessionID != "" {
			found = true
		}
		if strings.Contains(event.Reason, "private") || strings.Contains(event.Reason, "sensitive-user-command") {
			test.Fatal("session rejection exposed runtime output or command")
		}
	}
	if !found {
		test.Fatal("runtime rejection omitted its bound diagnostic")
	}
}
