package model

import "testing"

func TestHasServiceBinding(t *testing.T) {
	app := App{Bindings: []ServiceBinding{{ServiceID: " "}, {ServiceID: " db "}}}
	for _, tc := range []struct {
		id   string
		want bool
	}{{"  db ", true}, {"", false}, {"DB", false}, {"other", false}} {
		if got := app.HasServiceBinding(tc.id); got != tc.want {
			t.Errorf("HasServiceBinding(%q) = %t; want %t", tc.id, got, tc.want)
		}
	}
}

func TestMinObservedReplicaCount(t *testing.T) {
	for _, tc := range []struct {
		values []int
		want   int
	}{{nil, 0}, {[]int{7, 3, 5}, 3}, {[]int{5, -1, 8}, 0}, {[]int{0, 4}, 0}} {
		if got := MinObservedReplicaCount(tc.values...); got != tc.want {
			t.Errorf("MinObservedReplicaCount(%v) = %d; want %d", tc.values, got, tc.want)
		}
	}
}
