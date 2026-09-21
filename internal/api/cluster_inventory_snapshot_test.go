package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestNodeInventoryUsesOnePodSnapshotAndRetainsRestrictedFallback(t *testing.T) {
	for _, forbidden := range []bool{false, true} {
		var all, selected atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v1/nodes":
				fmt.Fprint(w, `{"items":[{"metadata":{"name":"worker"}}]}`)
			case "/api/v1/pods":
				if r.URL.Query().Get("labelSelector") == "" {
					all.Add(1)
					if forbidden {
						http.Error(w, "forbidden", 403)
						return
					}
				} else {
					selected.Add(1)
				}
				fmt.Fprint(w, `{"items":[{"metadata":{"name":"app","namespace":"sample","labels":{"app.kubernetes.io/managed-by":"fugue","app.kubernetes.io/name":""}},"spec":{"nodeName":"worker"},"status":{"phase":"Running"}}]}`)
			default:
				fmt.Fprint(w, `{}`)
			}
		}))
		client := &clusterNodeClient{client: server.Client(), baseURL: server.URL}
		rows, err := client.listClusterNodeInventory(context.Background())
		server.Close()
		if err != nil || len(rows) != 1 || len(rows[0].pods) != 1 || all.Load() != 1 {
			t.Fatalf("snapshot lost managed pod: %v %v all=%d", rows, err, all.Load())
		}
		want := int64(0)
		if forbidden {
			want = 2
		}
		if selected.Load() != want {
			t.Fatalf("redundant or missing filtered reads: %d want %d", selected.Load(), want)
		}
	}
}
