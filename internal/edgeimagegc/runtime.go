package edgeimagegc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"time"
)

type CommandRuntime struct {
	Client                      *http.Client
	APIURL, Namespace, HostRoot string
	runCRI                      func(context.Context, ...string) ([]byte, error)
}

func (r *CommandRuntime) command(ctx context.Context, args ...string) ([]byte, error) {
	if r.runCRI != nil {
		return r.runCRI(ctx, args...)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := append([]string{r.HostRoot, "/usr/local/bin/k3s", "crictl"}, args...)
	out, err := exec.CommandContext(ctx, "chroot", command...).Output()
	if err != nil {
		return nil, fmt.Errorf("crictl %s failed: %w", args[0], err)
	}
	if len(out) > 32<<20 {
		return nil, fmt.Errorf("CRI inventory exceeded bound")
	}
	return out, nil
}

func (r *CommandRuntime) Observe(ctx context.Context) (Evidence, error) {
	result := Evidence{Protected: map[string]bool{}}
	raw, err := r.command(ctx, "images", "--output", "json")
	if err != nil {
		return result, err
	}
	var images struct {
		Images []Image `json:"images"`
	}
	if err := json.Unmarshal(raw, &images); err != nil {
		return result, err
	}
	if images.Images == nil {
		return result, fmt.Errorf("CRI images inventory absent")
	}
	result.Images = images.Images
	raw, err = r.command(ctx, "ps", "-a", "--output", "json")
	if err != nil {
		return result, err
	}
	var containers struct {
		Containers []json.RawMessage `json:"containers"`
	}
	if err := json.Unmarshal(raw, &containers); err != nil || containers.Containers == nil {
		return result, fmt.Errorf("CRI container inventory absent")
	}
	if err := ProtectJSON(raw, result.Protected); err != nil {
		return result, err
	}
	// ConfigMaps include retained immutable release records, positive LKG and
	// desired/candidate artifacts. Keep every referenced digest conservatively.
	paths := []string{"/api/v1/pods", "/apis/apps/v1/deployments", "/apis/apps/v1/daemonsets", "/apis/apps/v1/statefulsets", "/apis/apps/v1/replicasets", "/apis/apps/v1/controllerrevisions", "/apis/batch/v1/jobs", "/apis/batch/v1/cronjobs", "/api/v1/namespaces/" + url.PathEscape(r.Namespace) + "/configmaps"}
	for _, path := range paths {
		continuation := ""
		count := 0
		for {
			query := url.Values{"limit": {"50"}}
			if continuation != "" {
				query.Set("continue", continuation)
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(r.APIURL, "/")+path+"?"+query.Encode(), nil)
			if err != nil {
				return result, err
			}
			response, err := r.Client.Do(request)
			if err != nil {
				return result, err
			}
			raw, err := io.ReadAll(io.LimitReader(response.Body, (16<<20)+1))
			_ = response.Body.Close()
			if err != nil || response.StatusCode != http.StatusOK || len(raw) > 16<<20 {
				return result, fmt.Errorf("Kubernetes inventory %s unavailable (status=%d)", path, response.StatusCode)
			}
			var page struct {
				Metadata struct {
					Continue string `json:"continue"`
				} `json:"metadata"`
				Items []json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(raw, &page); err != nil {
				return result, err
			}
			if page.Items == nil {
				return result, fmt.Errorf("Kubernetes inventory items absent")
			}
			count += len(page.Items)
			if count > 10000 {
				return result, fmt.Errorf("Kubernetes inventory %s exceeded bound", path)
			}
			for _, item := range page.Items {
				if err := ProtectJSON(item, result.Protected); err != nil {
					return result, err
				}
			}
			continuation = page.Metadata.Continue
			if continuation == "" {
				break
			}
		}
	}
	return result, nil
}

func (r *CommandRuntime) Remove(ctx context.Context, id string) error {
	if len(id) != 71 || digestPattern.FindString(id) != id {
		return fmt.Errorf("invalid exact image ID")
	}
	_, err := r.command(ctx, "rmi", id)
	return err
}
