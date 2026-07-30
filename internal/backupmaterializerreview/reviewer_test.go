package backupmaterializerreview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/backupmaterializeridentity"

	authenticationv1 "k8s.io/api/authentication/v1"
)

const (
	testPresentedToken = "test-header.test-materializer-payload.test-materializer-signature"
	testAPIToken       = "test-header.test-api-caller-payload.test-api-caller-signature"
)

type rotatingCredentialSource struct {
	mu     sync.Mutex
	tokens []string
	calls  int
	err    error
}

func (source *rotatingCredentialSource) Credential(context.Context) (string, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.calls++
	if source.err != nil {
		return "", source.err
	}
	index := source.calls - 1
	if index >= len(source.tokens) {
		index = len(source.tokens) - 1
	}
	if index < 0 {
		return "", nil
	}
	return source.tokens[index], nil
}

func TestReviewerPostsExactAudienceAndReturnsTokenFreeResult(t *testing.T) {
	t.Parallel()
	source := &rotatingCredentialSource{tokens: []string{testAPIToken}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.RequestURI() != tokenReviewPath ||
			request.Header.Get("Accept") != "application/json" || request.Header.Get("Accept-Encoding") != "identity" ||
			request.Header.Get("Authorization") != "Bearer "+testAPIToken || request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("TokenReview request boundary drifted: method=%s uri=%s headers=%v", request.Method, request.URL.RequestURI(), request.Header)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var review authenticationv1.TokenReview
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&review); err != nil || review.APIVersion != "authentication.k8s.io/v1" ||
			review.Kind != "TokenReview" || review.Spec.Token != testPresentedToken ||
			!reflect.DeepEqual(review.Spec.Audiences, []string{backupmaterializeridentity.Audience}) {
			t.Errorf("TokenReview request body drifted: review=%+v err=%v", review, err)
			http.Error(w, "bad review", http.StatusBadRequest)
			return
		}
		response := validTokenReviewResponse(review)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	reviewer, err := New(Config{APIServerURL: server.URL, CredentialSource: source, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("create reviewer: %v", err)
	}
	result, err := reviewer.ReviewToken(context.Background(), testPresentedToken, []string{backupmaterializeridentity.Audience})
	if err != nil {
		t.Fatalf("review materializer token: %v", err)
	}
	if !result.Authenticated || result.Username != "system:serviceaccount:fugue-system:fugue-backup-materializer-app-database-0123456789abcdef" ||
		result.UID != "11111111-1111-4111-8111-111111111111" ||
		!reflect.DeepEqual(result.Audiences, []string{backupmaterializeridentity.Audience}) ||
		len(result.Groups) != 3 || result.Extra["authentication.kubernetes.io/pod-name"][0] == "" || result.Error != "" || source.calls != 1 {
		t.Fatalf("review result drifted: result=%+v sourceCalls=%d", result, source.calls)
	}
	rendered := fmt.Sprintf("%+v", result)
	if strings.Contains(rendered, testPresentedToken) || strings.Contains(rendered, testAPIToken) || strings.Contains(rendered, "test-header") {
		t.Fatalf("review result retained a bearer token: %s", rendered)
	}
}

func TestReviewerRereadsRotatedCallerCredential(t *testing.T) {
	t.Parallel()
	secondAPIToken := "rotated-header.rotated-api-caller-payload.rotated-signature"
	source := &rotatingCredentialSource{tokens: []string{testAPIToken, secondAPIToken}}
	var mu sync.Mutex
	authorizations := []string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		mu.Unlock()
		var review authenticationv1.TokenReview
		_ = json.NewDecoder(request.Body).Decode(&review)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(validTokenReviewResponse(review))
	}))
	defer server.Close()
	reviewer, err := New(Config{APIServerURL: server.URL, CredentialSource: source, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("create reviewer: %v", err)
	}
	for range 2 {
		if _, err := reviewer.ReviewToken(context.Background(), testPresentedToken, []string{backupmaterializeridentity.Audience}); err != nil {
			t.Fatalf("review rotated caller credential: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"Bearer " + testAPIToken, "Bearer " + secondAPIToken}
	if !reflect.DeepEqual(authorizations, want) || source.calls != 2 {
		t.Fatalf("caller credential did not rotate: auth=%v calls=%d", authorizations, source.calls)
	}
}

func TestReviewerFailsClosedOnHTTPAndResponseDrift(t *testing.T) {
	t.Parallel()
	tests := map[string]func(http.ResponseWriter, *http.Request){
		"wrong status": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			http.Error(w, `{}`, http.StatusUnauthorized)
		},
		"wrong content type": func(w http.ResponseWriter, request *http.Request) {
			var review authenticationv1.TokenReview
			_ = json.NewDecoder(request.Body).Decode(&review)
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(validTokenReviewResponse(review))
		},
		"empty body": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
		},
		"trailing body": func(w http.ResponseWriter, request *http.Request) {
			var review authenticationv1.TokenReview
			_ = json.NewDecoder(request.Body).Decode(&review)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(validTokenReviewResponse(review))
			_, _ = w.Write([]byte(`{}`))
		},
		"unknown field": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","unexpected":true}`))
		},
		"oversized": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(strings.Repeat("x", 2049)))
		},
		"wrong kind": func(w http.ResponseWriter, request *http.Request) {
			var review authenticationv1.TokenReview
			_ = json.NewDecoder(request.Body).Decode(&review)
			response := validTokenReviewResponse(review)
			response.Kind = "Other"
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(response)
		},
		"spec token drift": func(w http.ResponseWriter, request *http.Request) {
			var review authenticationv1.TokenReview
			_ = json.NewDecoder(request.Body).Decode(&review)
			response := validTokenReviewResponse(review)
			response.Spec.Token = testAPIToken
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(response)
		},
		"spec audience drift": func(w http.ResponseWriter, request *http.Request) {
			var review authenticationv1.TokenReview
			_ = json.NewDecoder(request.Body).Decode(&review)
			response := validTokenReviewResponse(review)
			response.Spec.Audiences = []string{"other"}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(response)
		},
		"presented credential echo": func(w http.ResponseWriter, request *http.Request) {
			var review authenticationv1.TokenReview
			_ = json.NewDecoder(request.Body).Decode(&review)
			response := validTokenReviewResponse(review)
			response.Status.User.Username = "unexpected:" + testPresentedToken
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(response)
		},
		"caller credential echo": func(w http.ResponseWriter, request *http.Request) {
			var review authenticationv1.TokenReview
			_ = json.NewDecoder(request.Body).Decode(&review)
			response := validTokenReviewResponse(review)
			response.Status.User.Extra["unexpected"] = authenticationv1.ExtraValue{"unexpected:" + testAPIToken}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(response)
		},
	}
	for name, handler := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(handler))
			defer server.Close()
			reviewer, err := New(Config{
				APIServerURL: server.URL, CredentialSource: &rotatingCredentialSource{tokens: []string{testAPIToken}},
				HTTPClient: server.Client(), MaxResponseBytes: 2048,
			})
			if err != nil {
				t.Fatalf("create reviewer: %v", err)
			}
			_, err = reviewer.ReviewToken(context.Background(), testPresentedToken, []string{backupmaterializeridentity.Audience})
			assertNoCredentialLeak(t, err)
			if name == "wrong status" {
				if !errors.Is(err, ErrReviewerUnavailable) {
					t.Fatalf("review error = %v, want unavailable", err)
				}
				return
			}
			if !errors.Is(err, ErrReviewerResponse) {
				t.Fatalf("review error = %v, want invalid response", err)
			}
		})
	}
}

func TestReviewerRedactsRemoteStatusError(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var review authenticationv1.TokenReview
		_ = json.NewDecoder(request.Body).Decode(&review)
		response := validTokenReviewResponse(review)
		response.Status.Authenticated = false
		response.Status.Error = "remote detail contained " + testPresentedToken + " and " + testAPIToken
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	reviewer, err := New(Config{
		APIServerURL: server.URL, CredentialSource: &rotatingCredentialSource{tokens: []string{testAPIToken}},
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("create reviewer: %v", err)
	}
	result, err := reviewer.ReviewToken(context.Background(), testPresentedToken, []string{backupmaterializeridentity.Audience})
	if err != nil {
		t.Fatalf("review denied token: %v", err)
	}
	if result.Authenticated || result.Error != reviewDeniedMarker {
		t.Fatalf("remote status error was not reduced to its fixed marker: %+v", result)
	}
	rendered := fmt.Sprintf("%+v", result)
	if strings.Contains(rendered, testPresentedToken) || strings.Contains(rendered, testAPIToken) {
		t.Fatalf("redacted review result retained credential material: %s", rendered)
	}
}

func TestReviewerRejectsRedirectCredentialAndConfigDrift(t *testing.T) {
	t.Parallel()
	redirectTargetCalled := false
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectTargetCalled = true
	}))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	reviewer, err := New(Config{
		APIServerURL: redirect.URL, CredentialSource: &rotatingCredentialSource{tokens: []string{testAPIToken}},
		HTTPClient: redirect.Client(),
	})
	if err != nil {
		t.Fatalf("create redirect reviewer: %v", err)
	}
	if _, err := reviewer.ReviewToken(context.Background(), testPresentedToken, []string{backupmaterializeridentity.Audience}); !errors.Is(err, ErrReviewerUnavailable) {
		t.Fatalf("redirect review error = %v, want unavailable", err)
	}
	if redirectTargetCalled {
		t.Fatal("TokenReview redirect forwarded an API or materializer credential")
	}
	for name, config := range map[string]Config{
		"empty URL":       {CredentialSource: &rotatingCredentialSource{}},
		"plaintext":       {APIServerURL: "http://kubernetes.default.svc", CredentialSource: &rotatingCredentialSource{}},
		"URL credentials": {APIServerURL: "https://user@kubernetes.default.svc", CredentialSource: &rotatingCredentialSource{}},
		"URL path":        {APIServerURL: "https://kubernetes.default.svc/api", CredentialSource: &rotatingCredentialSource{}},
		"URL query":       {APIServerURL: "https://kubernetes.default.svc?x=1", CredentialSource: &rotatingCredentialSource{}},
		"empty URL query": {APIServerURL: "https://kubernetes.default.svc?", CredentialSource: &rotatingCredentialSource{}},
		"nil credential":  {APIServerURL: "https://kubernetes.default.svc"},
		"short timeout":   {APIServerURL: "https://kubernetes.default.svc", CredentialSource: &rotatingCredentialSource{}, RequestTimeout: time.Millisecond},
		"large timeout":   {APIServerURL: "https://kubernetes.default.svc", CredentialSource: &rotatingCredentialSource{}, RequestTimeout: 16 * time.Second},
		"small response":  {APIServerURL: "https://kubernetes.default.svc", CredentialSource: &rotatingCredentialSource{}, MaxResponseBytes: 100},
		"large response":  {APIServerURL: "https://kubernetes.default.svc", CredentialSource: &rotatingCredentialSource{}, MaxResponseBytes: 2 << 20},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(config); !errors.Is(err, ErrReviewerConfig) {
				t.Fatalf("config error = %v, want invalid config", err)
			}
		})
	}
}

func TestReviewerRejectsCredentialAndAudienceDriftBeforeNetwork(t *testing.T) {
	t.Parallel()
	source := &rotatingCredentialSource{tokens: []string{testAPIToken}}
	reviewer, err := New(Config{APIServerURL: "https://kubernetes.default.svc", CredentialSource: source})
	if err != nil {
		t.Fatalf("create reviewer: %v", err)
	}
	for name, invoke := range map[string]func() error{
		"nil context": func() error {
			_, err := reviewer.ReviewToken(nil, testPresentedToken, []string{backupmaterializeridentity.Audience})
			return err
		},
		"opaque token": func() error {
			_, err := reviewer.ReviewToken(context.Background(), "opaque", []string{backupmaterializeridentity.Audience})
			return err
		},
		"extra audience": func() error {
			_, err := reviewer.ReviewToken(context.Background(), testPresentedToken, []string{backupmaterializeridentity.Audience, "other"})
			return err
		},
		"wrong audience": func() error {
			_, err := reviewer.ReviewToken(context.Background(), testPresentedToken, []string{"other"})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invoke(); !errors.Is(err, ErrReviewerConfig) {
				t.Fatalf("review error = %v, want invalid config", err)
			}
		})
	}
	if source.calls != 0 {
		t.Fatalf("invalid request read the API caller credential %d time(s)", source.calls)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reviewer.ReviewToken(canceled, testPresentedToken, []string{backupmaterializeridentity.Audience}); !errors.Is(err, ErrReviewerUnavailable) {
		t.Fatalf("canceled review error = %v, want unavailable", err)
	}
	if source.calls != 0 {
		t.Fatalf("canceled request read the API caller credential %d time(s)", source.calls)
	}
	for name, source := range map[string]*rotatingCredentialSource{
		"source error": {err: errors.New("unavailable")},
		"empty token":  {tokens: []string{""}},
		"same token":   {tokens: []string{testPresentedToken}},
	} {
		t.Run(name, func(t *testing.T) {
			reviewer, err := New(Config{APIServerURL: "https://kubernetes.default.svc", CredentialSource: source})
			if err != nil {
				t.Fatalf("create reviewer: %v", err)
			}
			_, err = reviewer.ReviewToken(context.Background(), testPresentedToken, []string{backupmaterializeridentity.Audience})
			if name == "source error" {
				if !errors.Is(err, ErrReviewerUnavailable) {
					t.Fatalf("source error = %v, want unavailable", err)
				}
				return
			}
			if !errors.Is(err, ErrReviewerConfig) {
				t.Fatalf("source token error = %v, want invalid config", err)
			}
		})
	}
}

func TestReviewerDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list reviewer dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	if !reflect.DeepEqual(local, []string{"fugue/internal/backupmaterializeridentity"}) {
		t.Fatalf("TokenReview adapter dependency boundary widened: %v", local)
	}
	for _, forbidden := range []string{"k8s.io/client-go", "database/sql", "os", "os/exec"} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("TokenReview adapter gained forbidden dependency %q: %s", forbidden, output)
		}
	}
}

func validTokenReviewResponse(request authenticationv1.TokenReview) authenticationv1.TokenReview {
	return authenticationv1.TokenReview{
		TypeMeta: request.TypeMeta,
		Spec:     request.Spec,
		Status: authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{backupmaterializeridentity.Audience},
			User: authenticationv1.UserInfo{
				Username: "system:serviceaccount:fugue-system:fugue-backup-materializer-app-database-0123456789abcdef",
				UID:      "11111111-1111-4111-8111-111111111111",
				Groups: []string{
					"system:serviceaccounts",
					"system:serviceaccounts:fugue-system",
					"system:authenticated",
				},
				Extra: map[string]authenticationv1.ExtraValue{
					"authentication.kubernetes.io/credential-id": {"JTI=33333333-3333-4333-8333-333333333333"},
					"authentication.kubernetes.io/pod-name":      {"fugue-backup-materializer-app-database-0123456789abcdef-6d4f7c8b9f-abcde"},
					"authentication.kubernetes.io/pod-uid":       {"22222222-2222-4222-8222-222222222222"},
				},
			},
		},
	}
}

func assertNoCredentialLeak(t *testing.T, err error) {
	t.Helper()
	rendered := fmt.Sprint(err)
	if strings.Contains(rendered, testPresentedToken) || strings.Contains(rendered, testAPIToken) {
		t.Fatalf("review error retained credential material: %s", rendered)
	}
}
