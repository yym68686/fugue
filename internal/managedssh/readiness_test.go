package managedssh

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeVerificationRequiresLiveExecutionCapability(test *testing.T) {
	configuration, signer, _ := testConfiguration(test)
	executor := &testExecutor{}
	server, err := NewServer(executor, &testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), configuration, signer); err != nil {
		test.Fatal(err)
	}
	evidence, err := server.VerifyRuntime(context.Background(), "server-pod")
	if err != nil {
		test.Fatal(err)
	}
	if !evidence.RuntimeVerified || evidence.KeyExchangeVerified || evidence.ArtifactDigest != server.ConfigurationDigest() || evidence.ServerPodUID != "server-pod" || evidence.TargetPodUID != configuration.Target.PodUID || evidence.TargetContainerID != configuration.Target.ContainerID || evidence.ExpiresAt.Sub(evidence.ObservedAt) > time.Minute {
		test.Fatal("runtime evidence is unbound or claims an unperformed SSH handshake")
	}
	executor.validateError = errors.New("target execution unavailable")
	if _, err := server.VerifyRuntime(context.Background(), "server-pod"); err == nil {
		test.Fatal("configuration-only readiness reported runtime success")
	}
	if _, err := server.VerifyRuntime(context.Background(), ""); err == nil {
		test.Fatal("unidentified server issued evidence")
	}
}

type projectionRaceExecutor struct {
	testExecutor
	duringValidation func()
}

func (executor *projectionRaceExecutor) Validate(ctx context.Context, target Target) (Capabilities, error) {
	if executor.duringValidation != nil {
		executor.duringValidation()
	}
	return executor.testExecutor.Validate(ctx, target)
}

func TestRuntimeVerificationRecapturesAfterConcurrentProjection(test *testing.T) {
	configuration, signer, _ := testConfiguration(test)
	executor := &projectionRaceExecutor{}
	server, err := NewServer(executor, &testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), configuration, signer); err != nil {
		test.Fatal(err)
	}
	executor.duringValidation = func() {
		configuration.Generation++
		configuration.ValidUntil = configuration.ValidUntil.Add(time.Minute)
		if err := server.Activate(context.Background(), configuration, signer); err != nil {
			test.Fatal(err)
		}
	}
	if _, err := server.VerifyRuntime(context.Background(), "server-pod"); !errors.Is(err, ErrUnavailable) {
		test.Fatal("changed snapshot must not issue old evidence", err)
	}
	executor.duringValidation = nil
	evidence, err := server.VerifyRuntime(context.Background(), "server-pod")
	if err != nil || evidence.ArtifactDigest != server.ConfigurationDigest() {
		test.Fatal("fresh observation did not recover", err)
	}
}
