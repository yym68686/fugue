package objectstorage

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestEmptyBucketBatchIsBoundedAndResumable(t *testing.T) {
	objects, uploads, deletes, aborts := 9, 1, 0, 0
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/target-bucket" && r.URL.Path != "/target-bucket/pending" {
			t.Fatalf("wrong target %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == "GET" && r.URL.Query().Get("list-type") == "2":
			if r.URL.Query().Get("continuation-token") != "" {
				t.Error("used a cursor after removal")
			}
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
			if objects > 0 {
				fmt.Fprintf(w, `<Contents><Key>key-%d</Key><Size>10</Size></Contents>`, objects)
			}
			fmt.Fprint(w, `</ListBucketResult>`)
		case r.Method == "POST" && r.URL.Query().Has("delete"):
			var req struct {
				Objects []struct {
					Key string `xml:"Key"`
				} `xml:"Object"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Objects) != 1 || req.Objects[0].Key != fmt.Sprintf("key-%d", objects) {
				t.Error("wrong object batch")
			}
			objects--
			deletes++
			fmt.Fprint(w, `<DeleteResult/>`)
		case r.Method == "GET" && r.URL.Query().Has("uploads"):
			fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>`)
			if uploads > 0 {
				fmt.Fprint(w, `<Upload><Key>pending</Key><UploadId>upload-1</UploadId></Upload>`)
			}
			fmt.Fprint(w, `</ListMultipartUploadsResult>`)
		case r.Method == "DELETE" && r.URL.Query().Get("uploadId") == "upload-1":
			uploads--
			aborts++
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
			w.WriteHeader(400)
		}
	}))
	defer fake.Close()
	b := model.DataBackend{Bucket: "target-bucket", Endpoint: fake.URL, Region: "auto", Credentials: model.DataBackendCredentials{AccessKeyID: "synthetic-key", SecretAccessKey: "synthetic-secret"}}
	first, err := EmptyBucketBatch(context.Background(), b)
	if err != nil || first.Empty || first.DeletedObjects != 8 || aborts != 0 {
		t.Fatalf("first batch %+v err=%v", first, err)
	}
	second, err := EmptyBucketBatch(context.Background(), b)
	if err != nil || second.Empty || second.DeletedObjects != 1 || second.AbortedMultipartUploads != 1 {
		t.Fatalf("second batch %+v err=%v", second, err)
	}
	last, err := EmptyBucketBatch(context.Background(), b)
	if err != nil || !last.Empty || deletes != 9 || aborts != 1 {
		t.Fatalf("last batch %+v err=%v", last, err)
	}
}

func TestEmptyBucketBatchRejectsPerObjectErrorsAndSanitizes(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == "GET" {
			fmt.Fprint(w, `<ListBucketResult><Contents><Key>private-key</Key><Size>1</Size></Contents></ListBucketResult>`)
		} else {
			fmt.Fprint(w, `<DeleteResult><Error><Key>private-key</Key><Code>AccessDenied</Code><Message>private-detail</Message></Error></DeleteResult>`)
		}
	}))
	defer fake.Close()
	b := model.DataBackend{Bucket: "target", Endpoint: fake.URL, Credentials: model.DataBackendCredentials{AccessKeyID: "synthetic", SecretAccessKey: "synthetic"}}
	out, err := EmptyBucketBatch(context.Background(), b)
	if err == nil || out.Empty || out.DeletedObjects != 0 || strings.Contains(err.Error(), "private") {
		t.Fatalf("unsafe error out=%+v err=%v", out, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out, err := EmptyBucketBatch(ctx, b); err == nil || out.Empty {
		t.Fatal("cancellation reported success")
	}
}
