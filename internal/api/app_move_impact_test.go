package api

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestAppMoveOperationChainReflectsActualPrerequisites(t *testing.T) {
	tests := []struct {
		name   string
		impact model.AppMoveImpact
		want   []string
	}{
		{name: "stateless", want: []string{"switch", "cleanup"}},
		{
			name:   "database localization",
			impact: model.AppMoveImpact{Databases: []model.AppMoveDatabaseImpact{{RequiresLocalization: true}}},
			want:   []string{"database_localize", "switch", "cleanup"},
		},
		{
			name:   "movable rwo",
			impact: model.AppMoveImpact{Volumes: []model.AppMoveVolumeImpact{{Strategy: "rwo_snapshot_restore"}}},
			want:   []string{"quiesce", "snapshot_or_dump", "target_pvc_create", "restore", "permission_verify", "switch", "cleanup"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appMoveOperationChain(tt.impact); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("operation chain = %#v, want %#v", got, tt.want)
			}
		})
	}
}
