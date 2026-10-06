package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

//go:embed postgres_cold_probe.py
var coldProbeProgram string

type coldFileEvidence struct {
	SystemID      string `json:"system_id"`
	FileDigest    string `json:"file_digest"`
	ControlDigest string `json:"control_digest"`
	DataPath      string `json:"data_path"`
	Version       string `json:"version"`
	Files         int    `json:"files"`
	Bytes         int64  `json:"bytes"`
	UID           int64  `json:"uid"`
	GID           int64  `json:"gid"`
}

func coldExecConfig(c *kubeClient) (*rest.Config, error) {
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read Kubernetes exec CA: %w", err)
	}
	return &rest.Config{Host: c.baseURL, BearerToken: c.bearerToken, TLSClientConfig: rest.TLSClientConfig{CAData: ca}}, nil
}
func coldExec(ctx context.Context, c *kubeClient, ns, pod, container string, command []string, stdout io.Writer) error {
	cfg, err := coldExecConfig(c)
	if err != nil {
		return err
	}
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return err
	}
	u.Path = "/api/v1/namespaces/" + url.PathEscape(ns) + "/pods/" + url.PathEscape(pod) + "/exec"
	q := url.Values{"container": {container}, "stdout": {"true"}, "stderr": {"true"}, "stdin": {"false"}, "tty": {"false"}}
	q["command"] = command
	u.RawQuery = q.Encode()
	exec, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, u)
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: stdout, Stderr: &stderr}); err != nil {
		return fmt.Errorf("cold recovery exec %s/%s failed: %w: %.2000s", ns, pod, err, stderr.String())
	}
	return nil
}
func coldSourceEvidence(ctx context.Context, c *kubeClient, ns, pod string) (coldFileEvidence, error) {
	var out bytes.Buffer
	var ev coldFileEvidence
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := coldExec(ctx, c, ns, pod, "postgres", []string{"python3", "-c", coldProbeProgram, "source"}, &out); err != nil {
		return ev, err
	}
	if err := json.Unmarshal(out.Bytes(), &ev); err != nil {
		return ev, fmt.Errorf("invalid cold source evidence: %w", err)
	}
	if ev.SystemID == "" || len(ev.FileDigest) != 64 || len(ev.ControlDigest) != 64 || ev.Files < 1 || ev.Bytes < 1 || !strings.HasPrefix(ev.DataPath, "/") {
		return ev, fmt.Errorf("incomplete cold source file evidence")
	}
	return ev, nil
}
func execColdPostgresTar(ctx context.Context, c *kubeClient, ns, pod, dataPath, targetIP string) (string, error) {
	conn, err := (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, "tcp", net.JoinHostPort(targetIP, "8730"))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Minute))
	digest := sha256.New()
	// PGDATA is passed as an argv value, never interpolated into shell code.
	command := []string{"sh", "-ec", `data="$1"; rc=0; pg_ctl -D "$data" status >/dev/null 2>&1 || rc=$?; test "$rc" = 3; tar -cpf - -C "$(dirname "$data")" "$(basename "$data")"`, "cold-copy", dataPath}
	if err := coldExec(ctx, c, ns, pod, "postgres", command, io.MultiWriter(conn, digest)); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
