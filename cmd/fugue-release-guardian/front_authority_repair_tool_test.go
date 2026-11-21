package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestFrontRepairToolRequiresBoundedIsolatedWorkerScratch(t *testing.T) {
	limit := resource.MustParse("4Mi")
	yes, no := true, false
	worker := corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "edge",
		SecurityContext: &corev1.SecurityContext{ReadOnlyRootFilesystem: &yes, AllowPrivilegeEscalation: &no},
		VolumeMounts:    []corev1.VolumeMount{{Name: "scratch", MountPath: "/tmp"}}}},
		Volumes: []corev1.Volume{{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &limit}}}},
	}}
	if !frontRepairToolScratchAvailable(worker) {
		t.Fatal("declared bounded EmptyDir is not recognized")
	}
	for _, alter := range []func(*corev1.Pod){
		func(p *corev1.Pod) { p.Spec.Volumes[0].EmptyDir.SizeLimit = nil },
		func(p *corev1.Pod) { p.Spec.Volumes[0].EmptyDir.Medium = corev1.StorageMediumMemory },
		func(p *corev1.Pod) { p.Spec.Volumes[0].EmptyDir = nil },
		func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].ReadOnly = true },
		func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].SubPath = "other" },
		func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = &yes },
	} {
		modified := worker.DeepCopy()
		alter(modified)
		if frontRepairToolScratchAvailable(*modified) {
			t.Fatal("unsafe temporary executable volume was accepted")
		}
	}
}

func TestFrontRepairToolRunsExactCASWithoutLeavingAnInstalledCommand(t *testing.T) {
	if runtime.GOOS != "linux" || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		t.Skip("requires the production Linux process and procfs execution model")
	}
	binaryPath := filepath.Join(t.TempDir(), "cas")
	build := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-buildmode=exe", "-ldflags=-s -w", "-o", binaryPath, "../fugue-edge-front-cas")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("compile actual static Front CAS tool: %v: %s", err, output)
	}
	code, err := os.ReadFile(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	record := frontRepairToolRecord{SourceCommit: strings.Repeat("a", 40), Architecture: runtime.GOARCH, BinaryDigest: shaDigest(code), BinaryBytes: len(code)}
	tool, err := loadFrontRepairTool(record, code)
	if err != nil {
		t.Fatalf("tool does not fit production isolated scratch: size=%d err=%v", len(code), err)
	}
	otherArch := record
	otherArch.Architecture = "arm64"
	if runtime.GOARCH == "arm64" {
		otherArch.Architecture = "amd64"
	}
	if _, err := loadFrontRepairTool(otherArch, code); err == nil {
		t.Fatal("accepted a tool for a different node architecture")
	}
	before, nodes, observations := partialFrontRollbackFixture()
	plan, err := planPartialFrontRollbackRepair(before, "uid", "23", nodes, observations)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := sealFrontRepairJournal(plan, "sha256:"+strings.Repeat("b", 64), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	var divergent frontRepairMember
	for _, member := range plan.Members {
		if member.Activation.ActiveSlot != string(before.CurrentWorkerSlot) {
			divergent = member
		}
	}
	input := journal.activationRequest(divergent)
	stateFile := filepath.Join(t.TempDir(), "activation.json")
	raw, err := json.Marshal(divergent.Activation)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	run := func(phase string, bytesOnWire []byte) ([]byte, error) {
		t.Helper()
		write := edgegroupfront.ActivationRepairWrite{Schema: edgegroupfront.ActivationRepairWriteSchemaV1,
			ExpiresAt: time.Now().Add(8 * time.Second).UTC(), Request: input}
		encoded, err := json.Marshal(write)
		if err != nil {
			t.Fatal(err)
		}
		args, err := tool.command(phase, base64.RawURLEncoding.EncodeToString(encoded))
		if err != nil {
			t.Fatal(err)
		}
		// Only the test relocates the two production-fixed paths into fixtures.
		args[2] = strings.ReplaceAll(args[2], "/tmp", scratch)
		args[2] = strings.ReplaceAll(args[2], frontActivationStatePath, stateFile)
		command := exec.Command(args[0], args[1:]...)
		command.Stdin = bytes.NewReader(bytesOnWire)
		out, err := command.CombinedOutput()
		left, checkErr := os.ReadDir(scratch)
		if checkErr != nil || len(left) != 0 {
			t.Fatalf("temporary helper survived the exec: entries=%v err=%v", left, checkErr)
		}
		return out, err
	}
	corrupt := bytes.Clone(code)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := run("--repair-prepare-request", corrupt); err == nil {
		t.Fatal("corrupted helper executed")
	}
	if _, exists, err := edgegroupfront.ReadPublicationPreparation(stateFile); err != nil || exists {
		t.Fatalf("invalid delivery mutated the node: found=%v err=%v", exists, err)
	}
	ack, err := run("--repair-prepare-request", code)
	var prep edgegroupfront.PublicationPreparation
	if err != nil || decodeStrictJSON(ack, &prep) != nil || !frontRepairPreparationReady(prep, divergent.Activation, before, time.Now().UTC()) {
		t.Fatalf("temporary helper did not prepare exact committed Worker: %s err=%v", ack, err)
	}
	durable, _, err := edgegroupfront.ReadActivationState(stateFile)
	if err != nil || durable != divergent.Activation {
		t.Fatalf("preparation unexpectedly changed serving slot: %+v err=%v", durable, err)
	}
	ack, err = run("--repair-request", code)
	var activated edgegroupfront.ActivationReceipt
	if err != nil || decodeStrictJSON(ack, &activated) != nil || !input.MatchesActivation(activated.Current) {
		t.Fatalf("temporary helper did not execute the exact repair CAS: %s err=%v", ack, err)
	}
	if _, err := run("--repair-request", code); err == nil {
		t.Fatal("temporary helper accepted a replay of a changed predecessor")
	}
}
