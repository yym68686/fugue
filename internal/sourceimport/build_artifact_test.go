package sourceimport

import (
	"context"
	"errors"
	"testing"
)

func TestBuildArtifactRegistrationPrecedesExecutionAndCommitIsRequired(t *testing.T) {
	for _, fail := range []string{"register", "commit", ""} {
		t.Run(fail, func(t *testing.T) {
			events := []string{}
			ctx := WithBuildArtifactRecorder(context.Background(), func(_ context.Context, job, ref string, done bool) error {
				event := "register"
				if done {
					event = "commit"
				}
				events = append(events, event)
				if event == fail {
					return errors.New("storage unavailable")
				}
				return nil
			})
			err := runBuilderJobWithRetry(ctx, "build", "job", "registry.example/apps/sample:unique", nil, func(context.Context, builderJobAttempt) error { events = append(events, "execute"); return nil })
			if (err != nil) != (fail != "") {
				t.Fatalf("unexpected result %v", err)
			}
			want := 3
			if fail == "register" {
				want = 1
			}
			if len(events) != want || events[0] != "register" || (want == 3 && (events[1] != "execute" || events[2] != "commit")) {
				t.Fatalf("unsafe ordering: %v", events)
			}
		})
	}
}
