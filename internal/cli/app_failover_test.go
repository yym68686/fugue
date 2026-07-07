package cli

import (
	"reflect"
	"testing"
)

func TestDefaultAppContinuityCanaryStepWeightsStartAtInitialWeight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		initial int
		want    []int
	}{
		{name: "default", initial: 1, want: []int{1, 5, 25, 50, 100}},
		{name: "custom low", initial: 2, want: []int{2, 5, 25, 50, 100}},
		{name: "custom high", initial: 50, want: []int{50, 100}},
		{name: "between defaults", initial: 75, want: []int{75, 100}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := defaultAppContinuityCanaryStepWeights(tt.initial); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("step weights for initial %d = %+v, want %+v", tt.initial, got, tt.want)
			}
		})
	}
}
