package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"fugue/internal/releaseterminal"
)

const (
	githubAPIBaseURL    = "https://api.github.com"
	terminalRef         = "refs/heads/fugue-control-plane-release-terminal-state"
	terminalMatchingRef = "git/matching-refs/heads/fugue-control-plane-release-terminal-state"
	maxAPIResponseBytes = 256 << 10
)

var (
	oidPattern        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
)

type options struct {
	repository string
}

type readResult struct {
	SchemaVersion uint64                    `json:"schema_version"`
	Ref           string                    `json:"ref"`
	State         string                    `json:"state"`
	ObjectOID     string                    `json:"object_oid,omitempty"`
	Document      *releaseterminal.Document `json:"document,omitempty"`
}

type gitObject struct {
	SHA  string `json:"sha"`
	Type string `json:"type"`
}

type gitRef struct {
	Ref    string    `json:"ref"`
	Object gitObject `json:"object"`
}

type gitRefList []gitRef

func (refs *gitRefList) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return fmt.Errorf("GitHub ref list must be a non-null array")
	}
	var decoded []gitRef
	if err := json.Unmarshal(trimmed, &decoded); err != nil || decoded == nil {
		return fmt.Errorf("GitHub ref list must be a non-null array")
	}
	*refs = decoded
	return nil
}

type requiredGitObjectList struct {
	Values  []gitObject
	Present bool
}

func (objects *requiredGitObjectList) UnmarshalJSON(data []byte) error {
	objects.Present = true
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return fmt.Errorf("Git object list must be a non-null array")
	}
	var decoded []gitObject
	if err := json.Unmarshal(trimmed, &decoded); err != nil || decoded == nil {
		return fmt.Errorf("Git object list must be a non-null array")
	}
	objects.Values = decoded
	return nil
}

type gitCommit struct {
	SHA     string                `json:"sha"`
	Tree    gitObject             `json:"tree"`
	Parents requiredGitObjectList `json:"parents"`
}

type gitTreeEntry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
}

type gitTree struct {
	SHA       string         `json:"sha"`
	Truncated *bool          `json:"truncated"`
	Tree      []gitTreeEntry `json:"tree"`
}

type gitBlob struct {
	SHA      string `json:"sha"`
	Encoding string `json:"encoding"`
	Content  string `json:"content"`
	Size     *int64 `json:"size"`
}

type githubReader struct {
	baseURL    string
	repository string
	token      string
	client     *http.Client
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	parsed, err := parseOptions(args)
	if err != nil {
		_, _ = io.WriteString(stderr, "fugue-release-terminal-read: reader flags are invalid\n")
		return 1
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if token == "" {
		_, _ = io.WriteString(stderr, "fugue-release-terminal-read: GitHub token is unavailable\n")
		return 1
	}
	client := &http.Client{
		Timeout: 45 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	reader, err := newGitHubReader(githubAPIBaseURL, parsed.repository, token, client)
	if err != nil {
		_, _ = io.WriteString(stderr, "fugue-release-terminal-read: reader configuration is invalid\n")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	result, err := reader.Read(ctx)
	if err != nil {
		_, _ = io.WriteString(stderr, "fugue-release-terminal-read: terminal state read failed\n")
		return 1
	}
	encoded, err := encodeResult(result)
	if err != nil {
		_, _ = io.WriteString(stderr, "fugue-release-terminal-read: terminal state result is invalid\n")
		return 1
	}
	if _, err := stdout.Write(encoded); err != nil {
		_, _ = io.WriteString(stderr, "fugue-release-terminal-read: terminal state result write failed\n")
		return 1
	}
	return 0
}

func parseOptions(args []string) (options, error) {
	var parsed options
	flags := flag.NewFlagSet("fugue-release-terminal-read", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&parsed.repository, "repository", "", "GitHub owner/repository")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return options{}, fmt.Errorf("invalid arguments")
	}
	if !repositoryPattern.MatchString(parsed.repository) {
		return options{}, fmt.Errorf("repository must be owner/repository")
	}
	parts := strings.Split(parsed.repository, "/")
	for _, part := range parts {
		if part == "." || part == ".." || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".") {
			return options{}, fmt.Errorf("repository component is not canonical")
		}
	}
	return parsed, nil
}

func newGitHubReader(baseURL, repository, token string, client *http.Client) (*githubReader, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("GitHub API base URL is invalid")
	}
	if !repositoryPattern.MatchString(repository) || strings.TrimSpace(token) == "" || client == nil {
		return nil, fmt.Errorf("GitHub reader configuration is incomplete")
	}
	return &githubReader{
		baseURL:    strings.TrimRight(baseURL, "/"),
		repository: repository,
		token:      strings.TrimSpace(token),
		client:     client,
	}, nil
}

func (reader *githubReader) Read(ctx context.Context) (readResult, error) {
	beforeOID, beforePresent, err := reader.readRef(ctx)
	if err != nil {
		return readResult{}, err
	}
	if !beforePresent {
		afterOID, afterPresent, err := reader.readRef(ctx)
		if err != nil {
			return readResult{}, err
		}
		if afterPresent || afterOID != "" {
			return readResult{}, fmt.Errorf("terminal ref appeared during absent-state read")
		}
		return readResult{SchemaVersion: 1, Ref: terminalRef, State: "absent"}, nil
	}

	document, err := reader.readCarrier(ctx, beforeOID)
	if err != nil {
		return readResult{}, err
	}
	afterOID, afterPresent, err := reader.readRef(ctx)
	if err != nil {
		return readResult{}, err
	}
	if !afterPresent || afterOID != beforeOID {
		return readResult{}, fmt.Errorf("terminal ref changed during immutable carrier read")
	}
	return readResult{
		SchemaVersion: 1,
		Ref:           terminalRef,
		State:         "present",
		ObjectOID:     beforeOID,
		Document:      &document,
	}, nil
}

func (reader *githubReader) readRef(ctx context.Context) (string, bool, error) {
	var refs gitRefList
	if err := reader.getJSON(ctx, terminalMatchingRef, &refs); err != nil {
		return "", false, err
	}
	exact := make([]gitRef, 0, 1)
	for _, candidate := range refs {
		if candidate.Ref == terminalRef {
			exact = append(exact, candidate)
		}
	}
	if len(exact) == 0 {
		return "", false, nil
	}
	if len(exact) != 1 || exact[0].Object.Type != "commit" || !oidPattern.MatchString(exact[0].Object.SHA) {
		return "", false, fmt.Errorf("terminal ref is ambiguous or malformed")
	}
	return exact[0].Object.SHA, true, nil
}

func (reader *githubReader) readCarrier(ctx context.Context, objectOID string) (releaseterminal.Document, error) {
	var commit gitCommit
	if err := reader.getJSON(ctx, "git/commits/"+objectOID, &commit); err != nil {
		return releaseterminal.Document{}, err
	}
	if commit.SHA != objectOID || !oidPattern.MatchString(commit.Tree.SHA) || commit.Tree.Type != "" {
		return releaseterminal.Document{}, fmt.Errorf("terminal carrier commit identity is malformed")
	}
	if !commit.Parents.Present {
		return releaseterminal.Document{}, fmt.Errorf("terminal carrier parent list is missing")
	}
	parents := make([]string, len(commit.Parents.Values))
	for index, parent := range commit.Parents.Values {
		if !oidPattern.MatchString(parent.SHA) || (parent.Type != "" && parent.Type != "commit") {
			return releaseterminal.Document{}, fmt.Errorf("terminal carrier parent identity is malformed")
		}
		parents[index] = parent.SHA
	}

	var tree gitTree
	if err := reader.getJSON(ctx, "git/trees/"+commit.Tree.SHA, &tree); err != nil {
		return releaseterminal.Document{}, err
	}
	if tree.SHA != commit.Tree.SHA || tree.Truncated == nil || *tree.Truncated || len(tree.Tree) != 1 {
		return releaseterminal.Document{}, fmt.Errorf("terminal carrier tree is malformed or truncated")
	}
	entry := tree.Tree[0]
	if !oidPattern.MatchString(entry.SHA) {
		return releaseterminal.Document{}, fmt.Errorf("terminal carrier tree entry identity is malformed")
	}

	var blob gitBlob
	if err := reader.getJSON(ctx, "git/blobs/"+entry.SHA, &blob); err != nil {
		return releaseterminal.Document{}, err
	}
	if blob.SHA != entry.SHA || blob.Encoding != "base64" || blob.Size == nil || *blob.Size < 0 {
		return releaseterminal.Document{}, fmt.Errorf("terminal carrier blob identity is malformed")
	}
	payload, err := base64.StdEncoding.DecodeString(blob.Content)
	if err != nil || int64(len(payload)) != *blob.Size {
		return releaseterminal.Document{}, fmt.Errorf("terminal carrier blob payload is malformed")
	}

	entries := []releaseterminal.CarrierEntry{{
		Path: entry.Path,
		Mode: entry.Mode,
		Type: entry.Type,
		OID:  entry.SHA,
	}}
	return releaseterminal.ResolveCarrierSnapshot(releaseterminal.CarrierSnapshot{
		ObjectOID:  objectOID,
		ParentOIDs: parents,
		Entries:    entries,
		Payload:    payload,
	})
}

func (reader *githubReader) getJSON(ctx context.Context, endpoint string, target any) error {
	if endpoint == "" || strings.HasPrefix(endpoint, "/") || strings.Contains(endpoint, "..") || strings.ContainsAny(endpoint, "?#") {
		return fmt.Errorf("GitHub API endpoint is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		reader.baseURL+"/repos/"+reader.repository+"/"+endpoint, nil)
	if err != nil {
		return fmt.Errorf("construct GitHub API request")
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+reader.token)
	request.Header.Set("User-Agent", "fugue-release-terminal-reader/1")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := reader.client.Do(request)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return fmt.Errorf("GitHub API GET failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GitHub API GET returned status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxAPIResponseBytes+1))
	if err != nil || len(data) > maxAPIResponseBytes {
		return fmt.Errorf("GitHub API response is unreadable or oversized")
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("GitHub API response is invalid JSON")
	}
	return nil
}

func encodeResult(result readResult) ([]byte, error) {
	if result.SchemaVersion != 1 || result.Ref != terminalRef {
		return nil, fmt.Errorf("terminal result identity is invalid")
	}
	switch result.State {
	case "absent":
		if result.ObjectOID != "" || result.Document != nil {
			return nil, fmt.Errorf("absent terminal result contains an object")
		}
	case "present":
		if !oidPattern.MatchString(result.ObjectOID) || result.Document == nil {
			return nil, fmt.Errorf("present terminal result is incomplete")
		}
		if err := releaseterminal.Validate(*result.Document); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("terminal result state is unsupported")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}
