package main

import (
	"archive/zip"
	"bytes"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func frontRepairActionsFixture(source string, raw []byte) (frontRepairActionsPolicy, frontRepairArtifactIdentity, frontRepairActionsRun, frontRepairActionsJob, frontRepairActionsArtifact, time.Time) {
	now := time.Date(2026, 1, 4, 9, 0, 0, 0, time.UTC)
	policy := frontRepairActionsPolicy{RepositoryID: 3, WorkflowID: 5, SourceCommit: source}
	expected := frontRepairArtifactIdentity{RepositoryID: 3, WorkflowRunID: 10, ProducerJobID: 11,
		ArtifactID: 12, RunAttempt: 2, ArchiveDigest: shaDigest(raw)}
	repo := frontRepairActionsRepository{ID: 3}
	run := frontRepairActionsRun{ID: 10, WorkflowID: 5, RunAttempt: 2, Path: frontRepairActionsWorkflowPath,
		Event: "push", HeadBranch: "main", HeadSHA: source, Status: "completed",
		RunStartedAt: now.Add(-time.Hour), Repository: repo, HeadRepository: repo}
	job := frontRepairActionsJob{ID: 11, RunID: 10, RunAttempt: 2, HeadBranch: "main", HeadSHA: source,
		Name: "prepush", Status: "completed", Conclusion: "success", StartedAt: now.Add(-40 * time.Minute), CompletedAt: now.Add(-10 * time.Minute),
		Steps: []frontRepairActionsStep{
			{Name: frontRepairActionsBuildStep, Status: "completed", Conclusion: "success", StartedAt: now.Add(-35 * time.Minute), CompletedAt: now.Add(-20 * time.Minute)},
			{Name: frontRepairActionsUploadStep, Status: "completed", Conclusion: "success", StartedAt: now.Add(-20 * time.Minute), CompletedAt: now.Add(-18 * time.Minute)},
		}}
	notExpired := false
	artifact := frontRepairActionsArtifact{ID: 12, Digest: expected.ArchiveDigest, Size: int64(len(raw)),
		Name:    fmt.Sprintf("front-repair-tools-%s-%d-%d", source, run.ID, run.RunAttempt),
		Expired: &notExpired, CreatedAt: now.Add(-19 * time.Minute), ExpiresAt: now.Add(24 * time.Hour),
		WorkflowRun: frontRepairActionsArtifactRun{ID: 10, RepositoryID: 3, HeadRepositoryID: 3, HeadBranch: "main", HeadSHA: source}}
	return policy, expected, run, job, artifact, now
}

func TestFrontRepairArtifactRejectsUnprotectedOrDriftedActionsMetadata(t *testing.T) {
	source := strings.Repeat("a", 40)
	policy, expected, run, job, artifact, now := frontRepairActionsFixture(source, []byte("archive"))
	if err := validateFrontRepairActionsMetadata(policy, expected, run, job, artifact, now); err != nil {
		t.Fatalf("valid protected metadata: %v", err)
	}
	for name, alter := range map[string]func(*frontRepairActionsRun, *frontRepairActionsJob, *frontRepairActionsArtifact){
		"pull request": func(r *frontRepairActionsRun, _ *frontRepairActionsJob, _ *frontRepairActionsArtifact) {
			r.Event = "pull_request"
		},
		"fork": func(r *frontRepairActionsRun, _ *frontRepairActionsJob, _ *frontRepairActionsArtifact) {
			r.HeadRepository.ID++
		},
		"other workflow": func(r *frontRepairActionsRun, _ *frontRepairActionsJob, _ *frontRepairActionsArtifact) {
			r.WorkflowID++
		},
		"wrong job attempt": func(_ *frontRepairActionsRun, j *frontRepairActionsJob, _ *frontRepairActionsArtifact) {
			j.RunAttempt--
		},
		"wrong job": func(_ *frontRepairActionsRun, j *frontRepairActionsJob, _ *frontRepairActionsArtifact) { j.ID++ },
		"failed production plan": func(_ *frontRepairActionsRun, j *frontRepairActionsJob, _ *frontRepairActionsArtifact) {
			j.Conclusion = "failure"
		},
		"missing upload": func(_ *frontRepairActionsRun, j *frontRepairActionsJob, _ *frontRepairActionsArtifact) {
			j.Steps = j.Steps[:1]
		},
		"duplicate upload": func(_ *frontRepairActionsRun, j *frontRepairActionsJob, _ *frontRepairActionsArtifact) {
			j.Steps = append(j.Steps, j.Steps[1])
		},
		"wrong artifact attempt": func(_ *frontRepairActionsRun, _ *frontRepairActionsJob, a *frontRepairActionsArtifact) {
			a.Name = strings.TrimSuffix(a.Name, "-2") + "-1"
		},
		"other archive": func(_ *frontRepairActionsRun, _ *frontRepairActionsJob, a *frontRepairActionsArtifact) {
			a.Digest = shaDigest([]byte("other"))
		},
		"expired artifact": func(_ *frontRepairActionsRun, _ *frontRepairActionsJob, a *frontRepairActionsArtifact) {
			a.ExpiresAt = now
		},
		"expired missing": func(_ *frontRepairActionsRun, _ *frontRepairActionsJob, a *frontRepairActionsArtifact) {
			a.Expired = nil
		},
		"foreign artifact source": func(_ *frontRepairActionsRun, _ *frontRepairActionsJob, a *frontRepairActionsArtifact) {
			a.WorkflowRun.HeadSHA = strings.Repeat("b", 40)
		},
		"outside upload window": func(_ *frontRepairActionsRun, _ *frontRepairActionsJob, a *frontRepairActionsArtifact) {
			a.CreatedAt = now
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, j, a := run, job, artifact
			alter(&r, &j, &a)
			if validateFrontRepairActionsMetadata(policy, expected, r, j, a, now) == nil {
				t.Fatal("unsafe repair metadata accepted")
			}
		})
	}
}

type frontRepairZIPFixtureEntry struct {
	name string
	mode fs.FileMode
	data []byte
}

func frontRepairZIPFixture(t *testing.T, entries []frontRepairZIPFixtureEntry) []byte {
	t.Helper()
	var raw bytes.Buffer
	writer := zip.NewWriter(&raw)
	for _, f := range entries {
		header := &zip.FileHeader{Name: f.name, Method: zip.Deflate}
		header.SetMode(f.mode)
		entry, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(f.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return raw.Bytes()
}

func TestFrontRepairActionsArchiveRejectsUnexpectedEntriesWithoutExtraction(t *testing.T) {
	source := strings.Repeat("a", 40)
	m := frontRepairZIPFixtureEntry{"manifest.json", 0600, []byte("{}")}
	b := frontRepairZIPFixtureEntry{frontRepairToolArtifactPrefix + "amd64", 0500, []byte("fixture")}
	s := frontRepairZIPFixtureEntry{frontRepairToolArtifactPrefix + "arm64", fs.ModeSymlink | 0500, []byte(b.name)}
	for _, entries := range [][]frontRepairZIPFixtureEntry{
		{m, b, b},
		{m, b, {"../tool", 0500, []byte("fixture")}},
		{m, b, s},
		{m, b, {frontRepairToolArtifactPrefix + "arm64", 0500, make([]byte, frontRepairToolMaxBytes+1)}},
	} {
		if _, err := decodeFrontRepairActionsArchive(frontRepairZIPFixture(t, entries), source); err == nil {
			t.Fatalf("invalid ZIP inventory was accepted for %v", entries[2].name)
		}
	}
}

func frontRepairBuildFixtureArchive(t *testing.T) (string, frontRepairToolManifest, []byte, bool) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("rehearses the hosted Linux build and intake")
	}
	dir := t.TempDir()
	var source string
	dirty := false
	manifest := frontRepairToolManifest{Schema: frontRepairToolManifestSchema}
	entries := make([]frontRepairZIPFixtureEntry, 0, 3)
	for _, arch := range []string{"amd64", "arm64"} {
		name := frontRepairToolArtifactPrefix + arch
		path := filepath.Join(dir, name)
		build := exec.Command("go", "build", "-trimpath", "-buildvcs=true", "-buildmode=exe", "-ldflags=-s -w", "-o", path, "../fugue-edge-front-cas")
		build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+arch)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build repair helper for %s: %v: %s", arch, err, out)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		info, err := buildinfo.Read(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if source != "" && source != setting.Value {
					t.Fatal("cross-builds disagree on revision")
				}
				source = setting.Value
			case "vcs.modified":
				dirty = dirty || setting.Value == "true"
			}
		}
		manifest.Tools = append(manifest.Tools, frontRepairToolRecord{SourceCommit: source,
			Architecture: arch, BinaryDigest: shaDigest(data), BinaryBytes: len(data)})
		entries = append(entries, frontRepairZIPFixtureEntry{name, 0500, data})
	}
	manifest.SourceCommit = source
	var err error
	manifest.Digest, err = manifest.validatedDigest()
	if err != nil {
		t.Fatal(err)
	}
	m, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return source, manifest, frontRepairZIPFixture(t, append(entries, frontRepairZIPFixtureEntry{"manifest.json", 0600, m})), dirty
}

func TestFrontRepairActionsIntakeChecksRealCrossBuiltArchiveBytes(t *testing.T) {
	source, manifest, raw, dirty := frontRepairBuildFixtureArchive(t)
	policy, expected, run, job, artifact, now := frontRepairActionsFixture(source, raw)
	read := func(r any) *bytes.Reader {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		return bytes.NewReader(b)
	}
	accept, err := readFrontRepairActionsArtifact(policy, expected, read(run), read(job), read(artifact), bytes.NewReader(raw), now)
	if dirty {
		if err == nil {
			t.Fatal("dirty cross-builds accepted for production")
		}
	} else if err != nil || accept.identity != expected || accept.tools.digest != manifest.Digest || len(accept.tools.tools) != 2 {
		t.Fatalf("clean archive could not be consumed: %v", err)
	}
	raw[len(raw)-1] ^= 1
	if _, err := readFrontRepairActionsArtifact(policy, expected, read(run), read(job), read(artifact), bytes.NewReader(raw), now); err == nil {
		t.Fatal("archive tampering accepted")
	}
}
