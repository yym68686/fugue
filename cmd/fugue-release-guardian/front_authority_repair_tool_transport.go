package main

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"errors"
	"fmt"
	"runtime/debug"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

const frontRepairToolManifestSchema = "edge-front-repair-tools/v1"

type frontRepairToolManifest struct {
	Schema       string                  `json:"schema"`
	SourceCommit string                  `json:"sourceCommit"`
	Tools        []frontRepairToolRecord `json:"tools"`
	Digest       string                  `json:"digest"`
}

func (manifest frontRepairToolManifest) validatedDigest() (string, error) {
	if manifest.Schema != frontRepairToolManifestSchema || !exactSourceSHA(manifest.SourceCommit) ||
		len(manifest.Tools) < 1 || len(manifest.Tools) > 2 {
		return "", errors.New("Front repair tool build manifest is invalid")
	}
	last := ""
	for _, tool := range manifest.Tools {
		if (tool.Architecture != "amd64" && tool.Architecture != "arm64") || tool.Architecture <= last ||
			tool.SourceCommit != manifest.SourceCommit || !exactSHA256Digest(tool.BinaryDigest) ||
			tool.BinaryBytes <= 0 || tool.BinaryBytes > frontRepairToolMaxBytes {
			return "", errors.New("Front repair tool build manifest contains an unbound or duplicate tool")
		}
		last = tool.Architecture
	}
	manifest.Digest = ""
	raw, err := declarativerelease.CanonicalJSON(manifest)
	if err != nil {
		return "", err
	}
	return shaDigest(raw), nil
}

type frontRepairToolSet struct {
	digest, source string
	tools          map[string]frontRepairTool
}

// Inputs must come from the immutable CI artifact, not free-form paths in an
// approval. The recorded bytes, Go build identity, and signed/approved manifest
// digest are three distinct checks.
func loadFrontRepairToolSet(manifest frontRepairToolManifest, files map[string][]byte) (frontRepairToolSet, error) {
	digest, err := manifest.validatedDigest()
	if err != nil || digest != manifest.Digest || len(files) != len(manifest.Tools) {
		return frontRepairToolSet{}, errors.Join(errors.New("Front repair tool artifact inventory is not exact"), err)
	}
	set := frontRepairToolSet{digest: digest, source: manifest.SourceCommit, tools: make(map[string]frontRepairTool)}
	for _, record := range manifest.Tools {
		tool, err := loadFrontRepairTool(record, files[record.Architecture])
		if err != nil {
			return frontRepairToolSet{}, err
		}
		info, err := buildinfo.Read(bytes.NewReader(tool.code))
		if err != nil || !frontRepairToolBuildMatches(info, record) {
			return frontRepairToolSet{}, errors.Join(errors.New("Front repair tool lacks the exact clean build identity"), err)
		}
		set.tools[record.Architecture] = tool
	}
	return set, nil
}

func frontRepairToolBuildMatches(info *debug.BuildInfo, record frontRepairToolRecord) bool {
	if info == nil || info.Path != "fugue/cmd/fugue-edge-front-repair-cas" || info.Main.Path != "fugue" || len(info.Deps) != 0 {
		return false
	}
	wanted := map[string]string{"GOOS": "linux", "GOARCH": record.Architecture, "CGO_ENABLED": "0", "-buildmode": "exe",
		"vcs": "git", "vcs.revision": record.SourceCommit, "vcs.modified": "false"}
	seen := make(map[string]bool)
	for _, s := range info.Settings {
		if value, known := wanted[s.Key]; known {
			if seen[s.Key] || value != s.Value {
				return false
			}
			seen[s.Key] = true
		}
	}
	return len(seen) == len(wanted)
}

func (r frontRepairRuntime) stagedToolForMember(ctx context.Context, member frontRepairRuntimeMember) (frontRepairTool, error) {
	if r.nodeTools == nil || !frontRepairToolScratchAvailable(member.worker) {
		return frontRepairTool{}, fmt.Errorf("Worker for %s does not prove a bounded executable scratch volume", member.Node.Name)
	}
	node, err := r.activator.client.CoreV1().Nodes().Get(ctx, member.Node.Name, metav1.GetOptions{})
	if err != nil || string(node.UID) != member.Node.UID || !repairNodeReady(*node) {
		return frontRepairTool{}, errors.Join(fmt.Errorf("node %s changed before temporary tool delivery", member.Node.Name), err)
	}
	info := node.Status.NodeInfo
	if label := node.Labels["kubernetes.io/os"]; label != "" && label != info.OperatingSystem {
		return frontRepairTool{}, errors.New("node OS observation conflicts with its labels")
	}
	if label := node.Labels["kubernetes.io/arch"]; label != "" && label != info.Architecture {
		return frontRepairTool{}, errors.New("node architecture observation conflicts with its labels")
	}
	tool, exists := r.nodeTools.tools[info.Architecture]
	if info.OperatingSystem != "linux" || !exists {
		return frontRepairTool{}, fmt.Errorf("node %s lacks an approved binary for its current platform", member.Node.Name)
	}
	return tool, nil
}

// The staged path allows only the fixed digest-checking installer. It does
// not expose a general command+stdin surface to a repair declaration.
type frontRepairToolExecutor interface {
	execFrontRepairTool(context.Context, string, string, frontRepairTool, string, string) ([]byte, error)
}

func (executor *kubePodExecutor) execFrontRepairTool(ctx context.Context, namespace, pod string,
	tool frontRepairTool, mode, request string) ([]byte, error) {
	if executor == nil || executor.client == nil || executor.config == nil || namespace == "" || pod == "" {
		return nil, errors.New("Front repair tool target is invalid")
	}
	args, err := tool.command(mode, request)
	if err != nil {
		return nil, err
	}
	req := executor.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: "edge", Command: args, Stdin: true, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	stream, err := remotecommand.NewSPDYExecutor(executor.config, "POST", req.URL())
	if err != nil {
		return nil, errors.New("Front repair could not open a pinned Worker exec stream")
	}
	stdout, stderr := &frontRepairToolOutput{remaining: 64 << 10}, &frontRepairToolOutput{remaining: 4 << 10}
	err = stream.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: bytes.NewReader(tool.code), Stdout: stdout, Stderr: stderr,
	})
	if err != nil {
		// Remote errors may echo command arguments or pod-controlled output.
		return nil, errors.New("Front repair tool execution result is unproven")
	}
	if stdout.exceeded || stderr.exceeded || stdout.buf.Len() == 0 {
		return nil, errors.New("Front repair tool returned no bounded receipt")
	}
	return stdout.buf.Bytes(), nil
}

type frontRepairToolOutput struct {
	buf       bytes.Buffer
	remaining int
	exceeded  bool
}

func (output *frontRepairToolOutput) Write(p []byte) (int, error) {
	n := min(len(p), output.remaining)
	_, _ = output.buf.Write(p[:n])
	output.remaining -= n
	output.exceeded = output.exceeded || n < len(p)
	return len(p), nil
}
