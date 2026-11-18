package objectstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
)

const b2AuthorizeURL = "https://api.backblazeb2.com/b2api/v4/b2_authorize_account"

// B2 normalizes bucketInfo keys to lowercase before returning them.
const b2OwnershipField = "fuguebucket"

var b2APIEndpoint = regexp.MustCompile(`^https://api[0-9]+\.backblazeb2\.com$`)
var b2S3Endpoint = regexp.MustCompile(`^https://s3\.([a-z0-9-]+)\.backblazeb2\.com$`)
var b2KeyID = regexp.MustCompile(`^[a-zA-Z0-9]{12,128}$`)

type B2Client struct {
	http                        *http.Client
	AccountID, Endpoint, Region string
	api, token, managementID    string
	// Pinned on each resource request, not supplied by the API caller.
	BucketID string
}

type b2Error struct {
	status int
	code   string
}

func (e *b2Error) Error() string { return "Backblaze B2 rejected the request" }
func (e *b2Error) Unwrap() error { return &APIError{Status: e.status} }

func AuthorizeB2(ctx context.Context, keyID, key string, transport http.RoundTripper) (*B2Client, error) {
	client := &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b2AuthorizeURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(keyID, key)
	var result struct {
		AccountID string `json:"accountId"`
		Token     string `json:"authorizationToken"`
		APIInfo   struct {
			StorageAPI struct {
				URL     string `json:"apiUrl"`
				S3      string `json:"s3ApiUrl"`
				Allowed struct {
					Capabilities []string        `json:"capabilities"`
					Buckets      json.RawMessage `json:"buckets"`
					NamePrefix   *string         `json:"namePrefix"`
				} `json:"allowed"`
			} `json:"storageApi"`
		} `json:"apiInfo"`
	}
	if err := b2Request(client, req, &result); err != nil {
		return nil, err
	}
	info := result.APIInfo.StorageAPI
	apiURL, endpoint := strings.TrimSuffix(info.URL, "/"), strings.TrimSuffix(info.S3, "/")
	region := b2S3Endpoint.FindStringSubmatch(endpoint)
	if !b2APIEndpoint.MatchString(apiURL) || len(region) != 2 || !b2KeyID.MatchString(result.AccountID) || result.Token == "" {
		return nil, errors.New("Backblaze B2 returned invalid account endpoints")
	}
	if keyID == result.AccountID || string(info.Allowed.Buckets) != "null" || info.Allowed.NamePrefix != nil {
		return nil, errors.New("Backblaze B2 requires a non-master management key without bucket or filename restrictions")
	}
	for _, capability := range []string{"listBuckets", "writeBuckets", "deleteBuckets", "listKeys", "writeKeys", "deleteKeys", "listFiles", "readFiles", "writeFiles", "deleteFiles"} {
		if !slices.Contains(info.Allowed.Capabilities, capability) {
			return nil, errors.New("Backblaze B2 key is missing required bucket, key, or file capabilities")
		}
	}
	return &B2Client{http: client, AccountID: result.AccountID, Endpoint: endpoint, Region: region[1], api: apiURL, token: result.Token, managementID: keyID}, nil
}

func b2Request(client *http.Client, req *http.Request, out any) error {
	resp, err := client.Do(req)
	if err != nil {
		return &b2Error{status: http.StatusServiceUnavailable}
	}
	defer resp.Body.Close()
	r := io.LimitReader(resp.Body, 8<<20)
	if resp.StatusCode != http.StatusOK {
		var detail struct {
			Code string `json:"code"`
		}
		_ = json.NewDecoder(r).Decode(&detail)
		return &b2Error{status: resp.StatusCode, code: detail.Code}
	}
	if out == nil {
		_, err := io.Copy(io.Discard, r)
		return err
	}
	if json.NewDecoder(r).Decode(out) != nil {
		return &b2Error{status: http.StatusBadGateway}
	}
	return nil
}

func (c *B2Client) call(ctx context.Context, operation string, data map[string]any, out any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.api+"/b2api/v4/"+operation, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.token)
	req.Header.Set("Content-Type", "application/json")
	return b2Request(c.http, req, out)
}

type b2Bucket struct {
	ID   string            `json:"bucketId"`
	Name string            `json:"bucketName"`
	Type string            `json:"bucketType"`
	Info map[string]string `json:"bucketInfo"`
}

func (c *B2Client) findBucket(ctx context.Context, name string) (b2Bucket, error) {
	var page struct {
		Buckets []b2Bucket `json:"buckets"`
	}
	if err := c.call(ctx, "b2_list_buckets", map[string]any{"accountId": c.AccountID, "bucketName": name}, &page); err != nil {
		return b2Bucket{}, err
	}
	if page.Buckets == nil {
		return b2Bucket{}, errors.New("Backblaze B2 returned an incomplete bucket inventory")
	}
	if len(page.Buckets) == 0 {
		return b2Bucket{}, nil
	}
	b := page.Buckets[0]
	if len(page.Buckets) != 1 || b.Name != name || b.ID == "" || b.Type != "allPrivate" || b.Info[b2OwnershipField] != name || (c.BucketID != "" && b.ID != c.BucketID) {
		return b2Bucket{}, errors.New("Backblaze B2 bucket identity does not match the managed resource")
	}
	return b, nil
}

func (c *B2Client) ProvisionBucket(ctx context.Context, name string) (string, error) {
	b, err := c.findBucket(ctx, name)
	if err != nil || b.ID != "" {
		return b.ID, err
	}
	err = c.call(ctx, "b2_create_bucket", map[string]any{"accountId": c.AccountID, "bucketName": name, "bucketType": "allPrivate", "bucketInfo": map[string]string{b2OwnershipField: name}}, &b)
	if err != nil {
		return "", err
	}
	if b.ID == "" || b.Name != name || b.Type != "allPrivate" || b.Info[b2OwnershipField] != name {
		return "", errors.New("Backblaze B2 did not confirm the managed bucket identity")
	}
	return b.ID, nil
}

func (c *B2Client) BucketExists(ctx context.Context, name string) (bool, error) {
	b, err := c.findBucket(ctx, name)
	return b.ID != "", err
}

func (c *B2Client) DeleteBucket(ctx context.Context, name string) error {
	b, err := c.findBucket(ctx, name)
	if err != nil || b.ID == "" {
		return err
	}
	return c.call(ctx, "b2_delete_bucket", map[string]any{"accountId": c.AccountID, "bucketId": b.ID}, nil)
}

type b2Key struct {
	ID      string   `json:"applicationKeyId"`
	Name    string   `json:"keyName"`
	Secret  string   `json:"applicationKey"`
	Buckets []string `json:"bucketIds"`
}

// B2 only accepts alphanumeric characters and hyphens in key names.
func b2GrantName(name string) string { return "fugue-" + SecretAccessKey(name) }

func (c *B2Client) RevokeNamed(ctx context.Context, name string) error {
	name = b2GrantName(name)
	return c.scanKeys(ctx, func(k b2Key) error {
		if k.Name == name && len(k.Buckets) == 1 && (c.BucketID == "" || k.Buckets[0] == c.BucketID) {
			return c.Revoke(ctx, k.ID)
		}
		return nil
	})
}

func (c *B2Client) scanKeys(ctx context.Context, visit func(b2Key) error) error {
	input := map[string]any{"accountId": c.AccountID, "maxKeyCount": 1000}
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		var page struct {
			Keys []b2Key `json:"keys"`
			Next string  `json:"nextApplicationKeyId"`
		}
		if err := c.call(ctx, "b2_list_keys", input, &page); err != nil {
			return err
		}
		if page.Keys == nil {
			return errors.New("Backblaze B2 returned an incomplete application key inventory")
		}
		for _, k := range page.Keys {
			if err := visit(k); err != nil {
				return err
			}
		}
		if page.Next == "" {
			return nil
		}
		if seen[page.Next] {
			break
		}
		seen[page.Next] = true
		input["startApplicationKeyId"] = page.Next
	}
	return errors.New("Backblaze B2 key inventory did not complete; retry to reconcile")
}

func (c *B2Client) CreateCredential(ctx context.Context, name, bucket, permission string) (Token, error) {
	b, err := c.findBucket(ctx, bucket)
	if err != nil {
		return Token{}, err
	}
	if b.ID == "" || b.ID != c.BucketID {
		return Token{}, errors.New("Backblaze B2 managed bucket is unavailable")
	}
	// Permit HeadBucket, but don't reveal unrelated bucket names through ListBuckets.
	caps := []string{"listBuckets", "listFiles", "readFiles"}
	switch permission {
	case "read-only":
	case "read-write":
		caps = append(caps, "writeFiles", "deleteFiles")
	default:
		return Token{}, errors.New("invalid permission")
	}
	if err := c.RevokeNamed(ctx, name); err != nil {
		return Token{}, err
	}
	var k b2Key
	err = c.call(ctx, "b2_create_key", map[string]any{"accountId": c.AccountID, "keyName": b2GrantName(name), "bucketIds": []string{b.ID}, "capabilities": caps}, &k)
	if err != nil {
		return Token{}, err
	}
	if k.ID == "" || k.Secret == "" || k.Name != b2GrantName(name) || len(k.Buckets) != 1 || k.Buckets[0] != b.ID {
		return Token{}, errors.New("Backblaze B2 returned an invalid bucket credential; reconciliation required")
	}
	return Token{ID: k.ID, Name: k.Name, Value: k.Secret}, nil
}

func (c *B2Client) Revoke(ctx context.Context, id string) error {
	if !b2KeyID.MatchString(id) || id == c.managementID {
		return errors.New("invalid Backblaze B2 grant identity")
	}
	err := c.call(ctx, "b2_delete_key", map[string]any{"applicationKeyId": id}, nil)
	var upstream *b2Error
	if errors.As(err, &upstream) && (upstream.status == http.StatusNotFound || upstream.status == http.StatusBadRequest) {
		// Native API can report an unknown/deleted key as a 400. A
		// rejected request is only idempotent if inventory confirms absence.
		found := false
		listErr := c.scanKeys(ctx, func(k b2Key) error { found = found || k.ID == id; return nil })
		if listErr == nil && !found {
			return nil
		}
		return errors.Join(err, listErr)
	}
	return err
}
