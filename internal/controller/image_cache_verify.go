package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"fugue/internal/store"
)

const (
	destinationImageCacheVerifyTimeout  = 2 * time.Minute
	destinationImageCacheMaxResponseLen = 1 << 20
)

type destinationImageCacheVerification struct {
	Repo                string   `json:"repo"`
	Target              string   `json:"target"`
	Available           bool     `json:"available"`
	CanonicalDigest     string   `json:"canonical_digest"`
	ReferencedBlobs     []string `json:"referenced_blobs"`
	ReferencedManifests []string `json:"referenced_manifests"`
	ReferencedBlobBytes int64    `json:"referenced_blob_bytes"`
	Error               string   `json:"error"`
}

type destinationImageCacheVerifyFunc func(
	context.Context,
	string,
	string,
) (destinationImageCacheVerification, error)

func imageCacheManagementTokenFromEnv() string {
	return strings.TrimSpace(os.Getenv("FUGUE_BOOTSTRAP_ADMIN_KEY"))
}

func newDestinationImageCacheVerifier(managementToken string) destinationImageCacheVerifyFunc {
	managementToken = strings.TrimSpace(managementToken)
	return func(ctx context.Context, cacheEndpoint, managedImageRef string) (destinationImageCacheVerification, error) {
		if ctx == nil {
			ctx = context.Background()
		}
		if managementToken == "" {
			return destinationImageCacheVerification{}, fmt.Errorf("destination image-cache management token is unavailable")
		}
		verifyURL, err := destinationImageCacheVerifyURL(cacheEndpoint)
		if err != nil {
			return destinationImageCacheVerification{}, err
		}
		body, err := json.Marshal(map[string]string{"image_ref": strings.TrimSpace(managedImageRef)})
		if err != nil {
			return destinationImageCacheVerification{}, fmt.Errorf("encode destination image-cache verification: %w", err)
		}
		verifyCtx, cancel := context.WithTimeout(ctx, destinationImageCacheVerifyTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(verifyCtx, http.MethodPost, verifyURL, bytes.NewReader(body))
		if err != nil {
			return destinationImageCacheVerification{}, fmt.Errorf("create destination image-cache verification request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+managementToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		baseTransport, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return destinationImageCacheVerification{}, fmt.Errorf("destination image-cache HTTP transport is unavailable")
		}
		transport := baseTransport.Clone()
		transport.Proxy = nil
		transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
		defer transport.CloseIdleConnections()
		client := &http.Client{
			Transport: transport,
			Timeout:   destinationImageCacheVerifyTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return fmt.Errorf("destination image-cache verification redirect rejected")
			},
		}
		resp, err := client.Do(req)
		if err != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			return destinationImageCacheVerification{}, fmt.Errorf("request destination image-cache verification: %w", err)
		}
		defer resp.Body.Close()
		responseBody, err := io.ReadAll(io.LimitReader(resp.Body, destinationImageCacheMaxResponseLen+1))
		if err != nil {
			return destinationImageCacheVerification{}, fmt.Errorf("read destination image-cache verification: %w", err)
		}
		if len(responseBody) > destinationImageCacheMaxResponseLen {
			return destinationImageCacheVerification{}, fmt.Errorf("destination image-cache verification response is too large")
		}
		if resp.StatusCode != http.StatusOK {
			return destinationImageCacheVerification{}, fmt.Errorf("destination image-cache verification returned status %d", resp.StatusCode)
		}
		var result destinationImageCacheVerification
		decoder := json.NewDecoder(bytes.NewReader(responseBody))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&result); err != nil {
			return destinationImageCacheVerification{}, fmt.Errorf("decode destination image-cache verification: %w", err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return destinationImageCacheVerification{}, fmt.Errorf("decode destination image-cache verification: %w", err)
		}
		if err := validateDestinationImageCacheVerification(result, managedImageRef); err != nil {
			return destinationImageCacheVerification{}, err
		}
		return result, nil
	}
}

func destinationImageCacheVerifyURL(cacheEndpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(cacheEndpoint))
	if err != nil {
		return "", fmt.Errorf("parse destination image-cache endpoint: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", fmt.Errorf("destination image-cache endpoint is not an exact HTTP origin")
	}
	parsed.Path = "/fugue/cache/v1/verify"
	return parsed.String(), nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func validateDestinationImageCacheVerification(result destinationImageCacheVerification, managedImageRef string) error {
	repo, target, ok := managedImageRepoTarget(managedImageRef)
	if !ok || result.Repo != repo || result.Target != target {
		return fmt.Errorf("destination image-cache verification identity does not match imported image")
	}
	if !result.Available || strings.TrimSpace(result.Error) != "" {
		return fmt.Errorf("destination image-cache did not verify a complete local image graph")
	}
	canonicalDigest := store.CanonicalImageDigest(result.CanonicalDigest)
	if canonicalDigest == "" {
		return fmt.Errorf("destination image-cache verification returned no canonical digest")
	}
	if requestedDigest := store.CanonicalImageDigest(target); requestedDigest != "" && requestedDigest != canonicalDigest {
		return fmt.Errorf("destination image-cache verification returned a different canonical digest")
	}
	if result.ReferencedBlobBytes <= 0 || len(result.ReferencedBlobs) == 0 {
		return fmt.Errorf("destination image-cache verification returned no complete local blob graph")
	}
	seen := make(map[string]struct{}, len(result.ReferencedBlobs)+len(result.ReferencedManifests))
	for _, rawDigest := range append(append([]string(nil), result.ReferencedBlobs...), result.ReferencedManifests...) {
		digest := store.CanonicalImageDigest(rawDigest)
		if digest == "" {
			return fmt.Errorf("destination image-cache verification returned an invalid graph digest")
		}
		if _, exists := seen[digest]; exists {
			return fmt.Errorf("destination image-cache verification returned a duplicate graph digest")
		}
		seen[digest] = struct{}{}
	}
	return nil
}

func managedImageRepoTarget(imageRef string) (string, string, bool) {
	imageRef = strings.TrimSpace(imageRef)
	slash := strings.Index(imageRef, "/")
	if slash < 0 || slash+1 >= len(imageRef) {
		return "", "", false
	}
	repoTarget := imageRef[slash+1:]
	separator := strings.LastIndex(repoTarget, "@")
	if separator < 0 {
		separator = strings.LastIndex(repoTarget, ":")
	}
	if separator <= 0 || separator+1 >= len(repoTarget) {
		return "", "", false
	}
	repo := strings.Trim(repoTarget[:separator], "/")
	target := strings.TrimSpace(repoTarget[separator+1:])
	if repo == "" || target == "" || strings.ContainsAny(repo+target, " \t\r\n") {
		return "", "", false
	}
	return repo, target, true
}
