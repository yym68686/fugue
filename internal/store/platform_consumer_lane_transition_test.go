package store

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func TestConsumerLaneTransitionRetainsGlobalReplayCursor(t *testing.T) {
	now := time.Now().UTC()
	oldSet := model.PlatformExpectedConsumerSet{ID: "old-set", ReleaseSetID: "parent", ArtifactReleaseID: "shadow", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global"}
	nextSet := oldSet
	nextSet.ID, nextSet.ArtifactReleaseID = "next-set", "gray"
	old := model.PlatformArtifactRelease{ID: "shadow", ArtifactID: "parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", ReleaseChannel: "shadow", FencingToken: 9, ReleasedAt: now.Add(-time.Minute)}
	old.LaneKey = platformsafety.ReleaseLaneKey(old.ArtifactKind, old.ScopeKey, old.ReleaseChannel)
	next := old
	next.ID, next.ReleaseChannel, next.FencingToken, next.ReleasedAt, next.Status = "gray", "gray", 1, now, model.PlatformArtifactReleaseStatusActive
	next.LaneKey = platformsafety.ReleaseLaneKey(next.ArtifactKind, next.ScopeKey, next.ReleaseChannel)
	lane := model.PlatformReleaseLane{LaneKey: next.LaneKey, ActiveReleaseID: next.ID, FencingToken: 1}
	previous := model.PlatformConsumerInstance{IdentityVerified: true, ExpectedConsumerSetID: oldSet.ID, ReleaseSetID: oldSet.ReleaseSetID, FencingToken: old.FencingToken}
	cursor := &platformcontrol.PlatformConsumerHeartbeatCursor{Sequence: 40, GenerationSequence: 12, FencingToken: 9, IssuedAt: now.Add(-time.Second), RecentNonces: []string{"retained"}}
	got, err := consumerCursorForLaneTransition(previous, cursor, oldSet, nextSet, old, next, lane, 1)
	if err != nil || got.FencingToken != 1 || got.Sequence != 40 || got.GenerationSequence != 12 || !got.IssuedAt.Equal(cursor.IssuedAt) || !reflect.DeepEqual(got.RecentNonces, cursor.RecentNonces) || cursor.FencingToken != 9 {
		t.Fatal("lane transition reset global replay evidence", got, err)
	}
	// Different channels can legitimately have the same numeric fence.
	equalOld, equalPrevious, equalCursor := old, previous, *cursor
	equalOld.FencingToken, equalPrevious.FencingToken, equalCursor.FencingToken = 1, 1, 1
	if got, err := consumerCursorForLaneTransition(equalPrevious, &equalCursor, oldSet, nextSet, equalOld, next, lane, 1); err != nil || got.Sequence != cursor.Sequence {
		t.Fatal("equal numeric fence rejected a proven channel transition", err)
	}
	for name, mutate := range map[string]func(*model.PlatformArtifactRelease, *model.PlatformReleaseLane, *model.PlatformExpectedConsumerSet){
		"same lane": func(r *model.PlatformArtifactRelease, l *model.PlatformReleaseLane, s *model.PlatformExpectedConsumerSet) {
			r.ReleaseChannel = old.ReleaseChannel
			r.LaneKey = old.LaneKey
			l.LaneKey = old.LaneKey
		},
		"old publication": func(r *model.PlatformArtifactRelease, l *model.PlatformReleaseLane, s *model.PlatformExpectedConsumerSet) {
			r.ReleasedAt = old.ReleasedAt
		},
		"wrong parent": func(r *model.PlatformArtifactRelease, l *model.PlatformReleaseLane, s *model.PlatformExpectedConsumerSet) {
			r.ArtifactID = "other"
		},
		"frozen": func(r *model.PlatformArtifactRelease, l *model.PlatformReleaseLane, s *model.PlatformExpectedConsumerSet) {
			l.Frozen = true
		},
		"superseded lane": func(r *model.PlatformArtifactRelease, l *model.PlatformReleaseLane, s *model.PlatformExpectedConsumerSet) {
			l.ActiveReleaseID = "newer"
		},
		"wrong sequence": func(r *model.PlatformArtifactRelease, l *model.PlatformReleaseLane, s *model.PlatformExpectedConsumerSet) {
			l.FencingToken++
		},
		"wrong kind": func(r *model.PlatformArtifactRelease, l *model.PlatformReleaseLane, s *model.PlatformExpectedConsumerSet) {
			s.ArtifactKind = model.PlatformArtifactKindDNSAnswerBundle
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, l, s := next, lane, nextSet
			mutate(&r, &l, &s)
			if _, err := consumerCursorForLaneTransition(previous, cursor, oldSet, s, old, r, l, 1); err == nil {
				t.Fatal("unproven lane transition accepted")
			}
		})
	}
}
