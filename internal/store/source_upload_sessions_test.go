package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"fugue/internal/model"
	"sync"
	"testing"
)

func TestSourceSessionChunksPersistAndSubmissionReservationIsAtomic(t *testing.T) {
	path := t.TempDir() + "/state.json"
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := s.CreateTenant("Upload tenant")
	archive := bytes.Repeat([]byte("x"), SourceUploadChunkSize+9)
	hash := fmt.Sprintf("%x", sha256.Sum256(archive))
	v, err := s.CreateSourceUploadSession(model.SourceUploadSession{TenantID: tenant.ID, RequestID: "source-request", Filename: "archive.tgz", SizeBytes: int64(len(archive)), SHA256: hash})
	if err != nil {
		t.Fatal(err)
	}
	first := archive[:SourceUploadChunkSize]
	_, err = s.PutSourceUploadChunk(v.ID, 0, fmt.Sprintf("%x", sha256.Sum256(first)), first)
	if err != nil {
		t.Fatal(err)
	}
	s = New(path)
	if err = s.Init(); err != nil {
		t.Fatal(err)
	}
	v, err = s.GetSourceUploadSession(v.ID)
	if err != nil || len(v.Chunks) != 1 {
		t.Fatal(v, err)
	}
	if _, err = s.PutSourceUploadChunk(v.ID, 1, hash, []byte("changed")); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("wrong chunk accepted", err)
	}
	tail := archive[SourceUploadChunkSize:]
	if _, err = s.PutSourceUploadChunk(v.ID, 1, fmt.Sprintf("%x", sha256.Sum256(tail)), tail); err != nil {
		t.Fatal(err)
	}
	v, err = s.CompleteSourceUploadSession(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	started := make(chan bool, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := s.BeginSourceUploadSubmission(v.ID, hash)
			if err != nil {
				t.Error(err)
			}
			started <- ok
		}()
	}
	wg.Wait()
	close(started)
	count := 0
	for ok := range started {
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("started %d submissions", count)
	}
	// A lost handler after reservation remains explicitly uncertain and cannot be retried.
	recovered, ok, err := s.BeginSourceUploadSubmission(v.ID, hash)
	if err != nil || ok || recovered.State != "submitting" {
		t.Fatal(recovered, ok, err)
	}
}
