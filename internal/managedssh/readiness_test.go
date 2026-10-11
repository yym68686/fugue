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
