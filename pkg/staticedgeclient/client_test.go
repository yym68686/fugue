package staticedgeclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientEvidenceIsBoundedAndUnattested(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Fugue-Observation-ID", "server-fixture")
		w.WriteHeader(204)
	}))
	defer srv.Close()
	q := NewQueue(1)
	client := &http.Client{Transport: &Transport{Submit: q.Submit}}
	resp, e := client.Post(srv.URL, "application/octet-stream", strings.NewReader("client-fixture"))
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	v := <-q.C
	if v.Observation.Body.Bytes != 14 || v.ServerRequestID != "server-fixture" || v.Provenance != "client_reported_unattested" {
		t.Fatal(v)
	}
	q.Submit(v)
	if q.Submit(v) || q.Dropped() != 1 {
		t.Fatal("queue capacity not enforced")
	}
}
