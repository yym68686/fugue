package objectstorage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

type b2TestTransport func(*http.Request) (*http.Response, error)

func (f b2TestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const b2AuthFixture = `{
 "accountId":"012345abcdef","authorizationToken":"synthetic-session",
 "apiInfo":{"storageApi":{"apiUrl":"https://api004.backblazeb2.com","s3ApiUrl":"https://s3.us-west-004.backblazeb2.com",
 "allowed":{"buckets":null,"namePrefix":null,"capabilities":["listBuckets","writeBuckets","deleteBuckets","listKeys","writeKeys","deleteKeys","listFiles","readFiles","writeFiles","deleteFiles"]}}}
}`

func b2Response(code int, body string) (*http.Response, error) {
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
}

func TestAuthorizeB2RefusesRestrictedKeysAndUnsafeEndpoints(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, id, source, replacement string }{
		{"restricted", "12345678900001234", `"buckets":null`, `"buckets":[{"id":"1234"}]`},
		{"missing permissions", "12345678900001234", `"deleteKeys",`, ""},
		{"legacy master ID", "012345abcdef", "", ""},
		{"cluster-prefixed master ID", "004012345abcdef0000000000", "", ""},
		{"foreign host", "12345678900001234", "api004.backblazeb2.com", "api004.backblazeb2.com.example.invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := b2TestTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodGet || r.URL.String() != b2AuthorizeURL {
					t.Errorf("unexpected endpoint %s %s", r.Method, r.URL)
				}
				id, secret, ok := r.BasicAuth()
				if !ok || id != tc.id || secret != "synthetic-secret" {
					t.Error("expected only fixed-endpoint Basic auth")
				}
				return b2Response(200, strings.Replace(b2AuthFixture, tc.source, tc.replacement, 1))
			})
			if _, err := AuthorizeB2(ctx, tc.id, "synthetic-secret", mock); err == nil {
				t.Fatal("unsafe authorization accepted")
			}
		})
	}
}

func TestB2ManagedBucketAndScopedKeyLifecycle(t *testing.T) {
	const name = "fugue-objects-synthetic"
	const grant = "s3_grant_test"
	const bucket = `{"bucketId":"exact-bucket-id","bucketName":"` + name + `","bucketType":"allPrivate","bucketInfo":{"fuguebucket":"` + name + `"}}`
	const keyID = "00001234567890000000000001"
	const oldKeyID = "00001234567890000000000002"
	ctx := context.Background()
	var exists bool
	var removed []string
	mock := b2TestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() == b2AuthorizeURL {
			return b2Response(200, b2AuthFixture)
		}
		if r.Method != http.MethodPost || r.URL.Host != "api004.backblazeb2.com" || r.Header.Get("Authorization") != "synthetic-session" {
			t.Error("wrong Native API destination or auth")
		}
		var input map[string]any
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			t.Fatal("invalid Native API request")
		}
		switch strings.TrimPrefix(r.URL.Path, "/b2api/v4/") {
		case "b2_list_buckets":
			if input["accountId"] != "012345abcdef" || input["bucketName"] != name {
				t.Error("not scoped to named bucket")
			}
			if exists {
				return b2Response(200, `{"buckets":[`+bucket+`]}`)
			}
			return b2Response(200, `{"buckets":[]}`)
		case "b2_create_bucket":
			if input["bucketType"] != "allPrivate" || !reflect.DeepEqual(input["bucketInfo"], map[string]any{"fuguebucket": name}) {
				t.Error("bucket was not created private and marked")
			}
			exists = true
			return b2Response(200, bucket)
		case "b2_list_keys":
			switch input["startApplicationKeyId"] {
			case nil:
				return b2Response(200, `{"keys":[{"applicationKeyId":"`+keyID+`","keyName":"`+b2GrantName(grant)+`","bucketIds":["another-bucket"]}],"nextApplicationKeyId":"next"}`)
			case "next":
				return b2Response(200, `{"keys":[{"applicationKeyId":"`+oldKeyID+`","keyName":"`+b2GrantName(grant)+`","bucketIds":["exact-bucket-id"]}]}`)
			default:
				t.Error("unexpected key page")
			}
		case "b2_delete_key":
			removed = append(removed, input["applicationKeyId"].(string))
			return b2Response(200, `{}`)
		case "b2_create_key":
			if !reflect.DeepEqual(input["bucketIds"], []any{"exact-bucket-id"}) || !reflect.DeepEqual(input["capabilities"], []any{"listBuckets", "listFiles", "readFiles"}) {
				t.Error("grant exceeded requested scope")
			}
			return b2Response(200, `{"applicationKeyId":"`+keyID+`","applicationKey":"synthetic-raw-key","keyName":"`+b2GrantName(grant)+`","bucketIds":["exact-bucket-id"]}`)
		case "b2_delete_bucket":
			if input["bucketId"] != "exact-bucket-id" {
				t.Error("unbound deletion")
			}
			exists = false
			return b2Response(200, `{}`)
		}
		t.Errorf("unexpected provider call %s", r.URL)
		return b2Response(400, `{}`)
	})
	client, err := AuthorizeB2(ctx, "004012345abcdef0000000001", "synthetic-secret", mock)
	if err != nil || client.Region != "us-west-004" {
		t.Fatalf("authorization: %v", err)
	}
	for i := 0; i < 2; i++ {
		id, err := client.ProvisionBucket(ctx, name)
		if err != nil || id != "exact-bucket-id" {
			t.Fatalf("provision: %s %v", id, err)
		}
		client.BucketID = id
	}
	key, err := client.CreateCredential(ctx, grant, name, "read-only")
	if err != nil || key.ID != keyID || key.Value != "synthetic-raw-key" || !reflect.DeepEqual(removed, []string{oldKeyID}) {
		t.Fatalf("grant lifecycle error: %v", err)
	}
	client.BucketID = "foreign-id"
	if err := client.DeleteBucket(ctx, name); err == nil || !exists {
		t.Fatal("deleted with a mismatched bucket ID")
	}
	client.BucketID = "exact-bucket-id"
	if err := client.DeleteBucket(ctx, name); err != nil || exists {
		t.Fatalf("delete: %v", err)
	}
}
