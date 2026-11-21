package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestFrontRepairArtifactCheckDoesNotFetchArchiveForUntrustedProducer(t *testing.T) {
	source := strings.Repeat("a", 40)
	policy, identity, run, job, archive, now := frontRepairActionsFixture(source, []byte("fake archive"))
	const prefix = "repos/fixture/fugue/actions/"
	args := []string{"--repository", "fixture/fugue", "--source-commit", source,
		"--repository-id", fmt.Sprint(policy.RepositoryID), "--workflow-id", fmt.Sprint(policy.WorkflowID),
		"--run-id", fmt.Sprint(identity.WorkflowRunID), "--run-attempt", fmt.Sprint(identity.RunAttempt),
		"--producer-job-id", fmt.Sprint(identity.ProducerJobID), "--artifact-id", fmt.Sprint(identity.ArtifactID),
		"--archive-digest", identity.ArchiveDigest}
	paths := []string{prefix + "runs/10/attempts/2", prefix + "jobs/11", prefix + "artifacts/12"}
	for _, goodProducer := range []bool{false, true} {
		if !goodProducer {
			job.Conclusion = "failure"
		} else {
			job.Conclusion = "success"
		}
		var seen []string
		get := func(_ context.Context, endpoint string, bound int64) ([]byte, error) {
			seen = append(seen, endpoint)
			if endpoint == prefix+"artifacts/12/zip" {
				if !goodProducer || bound != archive.Size {
					t.Fatalf("untrusted or unbounded archive fetch (limit=%d)", bound)
				}
				return []byte("fake archive"), nil
			}
			if bound != frontRepairActionsMetadataMax {
				t.Fatalf("metadata was not bounded: %d", bound)
			}
			switch endpoint {
			case paths[0]:
				return json.Marshal(run)
			case paths[1]:
				return json.Marshal(job)
			case paths[2]:
				return json.Marshal(archive)
			default:
				t.Fatalf("unexpected endpoint %s", endpoint)
				return nil, nil
			}
		}
		want := paths
		if goodProducer {
			want = append(append([]string(nil), paths...), prefix+"artifacts/12/zip")
		}
		var out bytes.Buffer
		err := runFrontRepairArtifactCheckWithFetcher(args, &out, get, now)
		if err == nil || out.Len() != 0 || !reflect.DeepEqual(seen, want) {
			t.Fatalf("unverified producer or invalid archive got a receipt: endpoints=%v err=%v stdout=%q", seen, err, out.String())
		}
	}
	reject := func(_ context.Context, endpoint string, _ int64) ([]byte, error) {
		t.Fatalf("invalid flags reached GitHub read: %s", endpoint)
		return nil, nil
	}
	if err := runFrontRepairArtifactCheckWithFetcher(append(args, "--apply"), io.Discard, reject, now); err == nil {
		t.Fatal("artifact check accepted an apply option")
	}
	bad := append([]string(nil), args...)
	bad[1] = "fixture/fugue/../../"
	if err := runFrontRepairArtifactCheckWithFetcher(bad, io.Discard, reject, now); err == nil {
		t.Fatal("artifact check accepted an unscoped repository")
	}
}

func TestFrontRepairActionsOutputBound(t *testing.T) {
	out := &frontRepairBoundedOutput{remaining: 3}
	if got, err := out.Write([]byte("abc")); got != 3 || err != nil {
		t.Fatalf("exact length rejected: %d %v", got, err)
	}
	if got, err := out.Write([]byte("d")); got != 0 || err == nil || !out.exceeded || out.data.String() != "abc" {
		t.Fatalf("oversized GitHub response was retained: %d %v", got, err)
	}
}
