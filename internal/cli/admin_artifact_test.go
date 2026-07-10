package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestAdminArtifactReleaseSerializesKernelBreakGlassAuthorization(t *testing.T) {
	t.Parallel()

	var got model.PlatformArtifactReleaseRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/artifacts/artifact-1/release" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode release request: %v", err)
		}
		writeJSONResponse(t, w, platformArtifactReleaseEnvelope{
			Artifact: model.PlatformArtifact{ID: "artifact-1", Generation: "generation-1"},
			Release: model.PlatformArtifactRelease{
				ID:                "release-1",
				ArtifactID:        "artifact-1",
				Generation:        "generation-1",
				ReleaseChannel:    model.PlatformArtifactReleaseChannelFull,
				OverrideMode:      model.PlatformArtifactOverrideModeKernelBreakGlass,
				OverrideExpiresAt: &got.KernelBreakGlass.ExpiresAt,
				ReleasedAt:        time.Now().UTC(),
			},
		})
	}))
	defer server.Close()

	before := time.Now().UTC()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"admin", "artifact", "release", "artifact-1",
		"--channel", "full",
		"--kernel-break-glass",
		"--break-glass-ttl", "5m",
		"--confirm-kernel-bypass", platformsafety.KernelBreakGlassConfirmation,
		"--confirm-target", "artifact-1",
		"--reason", "emergency recovery test",
	}, &stdout, &stderr)
	after := time.Now().UTC()
	if err != nil {
		t.Fatalf("run kernel break-glass release: %v stderr=%s", err, stderr.String())
	}
	if got.KernelBreakGlass == nil ||
		got.KernelBreakGlass.Confirmation != platformsafety.KernelBreakGlassConfirmation ||
		got.KernelBreakGlass.TargetConfirmation != "artifact-1" ||
		got.SoftOverride ||
		got.ForcePublish {
		t.Fatalf("unexpected serialized kernel break-glass request: %+v", got)
	}
	if got.KernelBreakGlass.ExpiresAt.Before(before.Add(5*time.Minute-time.Second)) ||
		got.KernelBreakGlass.ExpiresAt.After(after.Add(5*time.Minute+time.Second)) {
		t.Fatalf("unexpected break-glass expiry: %s", got.KernelBreakGlass.ExpiresAt)
	}
}

func TestAdminArtifactReleaseNormalizesLegacyForcePublishToSoftOverride(t *testing.T) {
	t.Parallel()

	var got model.PlatformArtifactReleaseRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode release request: %v", err)
		}
		writeJSONResponse(t, w, platformArtifactReleaseEnvelope{
			Artifact: model.PlatformArtifact{ID: "artifact-1", Generation: "generation-1"},
			Release: model.PlatformArtifactRelease{
				ID:             "release-1",
				ArtifactID:     "artifact-1",
				Generation:     "generation-1",
				ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
				OverrideMode:   model.PlatformArtifactOverrideModeSoft,
				ReleasedAt:     time.Now().UTC(),
			},
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"admin", "artifact", "release", "artifact-1",
		"--channel", "shadow",
		"--force-publish",
		"--reason", "legacy compatibility",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run legacy force publish: %v stderr=%s", err, stderr.String())
	}
	if !got.SoftOverride || got.ForcePublish || got.KernelBreakGlass != nil {
		t.Fatalf("legacy force publish was not normalized to canonical soft override: %+v", got)
	}
}

func TestAdminArtifactReleaseRejectsInvalidBreakGlassLocally(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "request should not be sent", http.StatusInternalServerError)
	}))
	defer server.Close()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing-confirmations",
			args: []string{"--kernel-break-glass"},
			want: "--confirm-kernel-bypass and --confirm-target are required",
		},
		{
			name: "ttl-too-long",
			args: []string{
				"--kernel-break-glass",
				"--break-glass-ttl", "16m",
				"--confirm-kernel-bypass", platformsafety.KernelBreakGlassConfirmation,
				"--confirm-target", "artifact-1",
			},
			want: "--break-glass-ttl must be positive",
		},
		{
			name: "wrong-safety-confirmation",
			args: []string{
				"--kernel-break-glass",
				"--confirm-kernel-bypass", "BYPASS",
				"--confirm-target", "artifact-1",
			},
			want: "--confirm-kernel-bypass must exactly equal",
		},
		{
			name: "wrong-target-confirmation",
			args: []string{
				"--kernel-break-glass",
				"--confirm-kernel-bypass", platformsafety.KernelBreakGlassConfirmation,
				"--confirm-target", "artifact-2",
			},
			want: "--confirm-target must exactly match",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := []string{
				"--base-url", server.URL,
				"--token", "token",
				"admin", "artifact", "release", "artifact-1",
				"--channel", "full",
				"--reason", "test invalid local authorization",
			}
			args = append(args, test.args...)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			err := runWithStreams(args, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected local error containing %q, got %v", test.want, err)
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid break-glass commands sent %d HTTP requests", requests.Load())
	}
}
