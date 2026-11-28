package buildadmission

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func TestEvaluatePlacementRequiresEligibleAllocationAbovePreviousOOM(t *testing.T) {
	const limit = 6 << 30
	app := model.App{TenantID: "tenant", ID: "app"}
	source := model.AppSource{CommitSHA: strings.Repeat("a", 40), BuildStrategy: model.AppBuildStrategyAuto}
	spec := model.AppSpec{}
	unavailable := errors.New("placement unavailable")
	admissionErr := &sourceimport.BuilderMemoryAdmissionError{PreviousOOMBytes: limit}
	for _, tc := range []struct {
		name, profile, strategy string
		eligible, wantReject    bool
		allocation              int64
		probeErr, wantErr       error
	}{
		{name: "legacy unresolved"},
		{name: "equal limit", profile: "heavy", strategy: model.AppBuildStrategyDockerfile, eligible: true, allocation: limit, wantReject: true, probeErr: admissionErr},
		{name: "ineligible larger", profile: "light", strategy: model.AppBuildStrategyStaticSite, allocation: limit + 1, wantReject: true},
		{name: "eligible larger", profile: "light", strategy: model.AppBuildStrategyStaticSite, eligible: true, allocation: limit + 1},
		{name: "probe unavailable", profile: "heavy", strategy: model.AppBuildStrategyDockerfile, probeErr: unavailable, wantErr: unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := oomHistoryKey(app, OOMInputFingerprint(source, spec))
			raw, err := json.Marshal(OOMHistory{Version: 1, BuilderOOMObservation: sourceimport.BuilderOOMObservation{
				MemoryLimitBytes: limit, WorkloadProfile: tc.profile,
			}})
			if err != nil {
				t.Fatal(err)
			}
			admission := New(memoryCheckpoints{key: raw}, nil)
			called := false
			reject, err := admission.EvaluatePlacement(t.Context(), app, source, spec,
				func(ctx context.Context, strategy string, stateful bool, previous int64) (model.BuilderPlacementInspection, error) {
					called = true
					deadline, bounded := ctx.Deadline()
					if strategy != tc.strategy || stateful || previous != limit || !bounded || time.Until(deadline) > 8*time.Second {
						t.Fatalf("unexpected probe input: %q %v %d, deadline %v", strategy, stateful, previous, deadline)
					}
					return model.BuilderPlacementInspection{Nodes: []model.BuilderPlacementNodeInspection{{
						Eligible: tc.eligible, Allocation: model.BuilderResourceSnapshot{MemoryBytes: tc.allocation},
					}}}, tc.probeErr
				})
			if !errors.Is(err, tc.wantErr) || called != (tc.strategy != "") || (reject != nil) != tc.wantReject {
				t.Fatalf("called=%t rejection=%v error=%v", called, reject, err)
			}
			if reject != nil && (reject.PreviousOOMBytes != limit || reject.AllocatedBytes != tc.allocation) {
				t.Fatalf("unexpected rejection evidence: %+v", reject)
			}
		})
	}
}
