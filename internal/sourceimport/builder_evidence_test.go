package sourceimport

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBuilderAttemptCaptureAfterDeadlineIsSafeAndIndependent(t *testing.T) {
	dir := t.TempDir()
	script := `#!/bin/sh
case "$*" in
 *"get pods"*) cat <<'JSON'
{"items":[{"metadata":{"name":"private-builder","uid":"uid-current","creationTimestamp":"__CREATED__"},"spec":{"nodeName":"private-node","containers":[{"resources":{"requests":{"memory":"512Mi"},"limits":{"ephemeral-storage":"8Gi"}}}]},"status":{"phase":"Pending","conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable","message":"2 Insufficient memory; node private-node 10.42.1.1 secret=password"}]}}]}
JSON
 ;;
 *"get events"*)
 case "$*" in *"involvedObject.uid=uid-current"*) ;; *) exit 1;; esac
 printf '%s' '{"items":[{"reason":"Evicted","message":"Pod ephemeral local storage usage exceeds the total limit of containers 8Gi."}]}' ;;
 *) exit 1;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(strings.ReplaceAll(script, "__CREATED__", time.Now().UTC().Format(time.RFC3339))), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("POD_NAMESPACE", "private-namespace")
	var facts []model.BuilderAttemptEvidence
	ctx, cancel := context.WithCancel(context.Background())
	ctx = WithBuilderEvidenceRecorder(ctx, func(f model.BuilderAttemptEvidence, _ map[string]any) { facts = append(facts, f) })
	cancel()
	recordBuilderAttempt(ctx, "private-job", builderJobAttempt{Number: 1}, time.Now().Add(-time.Minute), context.DeadlineExceeded)
	if len(facts) != 1 || facts[0].EphemeralLimitBytes != 8*1024*1024*1024 || !hasBuilderCause(facts[0], "builder_memory_unavailable") || !hasBuilderCause(facts[0], "ephemeral_storage_limit_exceeded") {
		t.Fatalf("missing evidence %+v", facts)
	}
	data, _ := json.Marshal(BuilderAttemptPayload(facts[0]))
	for _, private := range []string{"private-node", "10.42.1.1", "private-namespace", "password", "private-job"} {
		if strings.Contains(string(data), private) {
			t.Fatalf("public evidence leaks %s", private)
		}
	}
	recordBuilderAttempt(ctx, "private-job", builderJobAttempt{Number: 2}, time.Now(), errors.New("unknown error private-node"))
	if len(facts) != 2 || facts[1].Attempt != 2 {
		t.Fatal("retry overwrote first attempt")
	}
}
func TestBuilderCaptureMissingDoesNotInventReason(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("POD_NAMESPACE", "test")
	var fact model.BuilderAttemptEvidence
	ctx := WithBuilderEvidenceRecorder(context.Background(), func(f model.BuilderAttemptEvidence, _ map[string]any) { fact = f })
	recordBuilderAttempt(ctx, "job", builderJobAttempt{Number: 1}, time.Now(), errors.New("unclassified"))
	if !hasBuilderCause(fact, "evidence_unavailable") || len(fact.MissingEvidence) == 0 {
		t.Fatalf("must retain evidence gap %+v", fact)
	}
}
