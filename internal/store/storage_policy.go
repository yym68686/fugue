package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"fugue/internal/inputhistory"
)

// StoragePolicy is a independently published runtime constraint, not serving
// authority. Readers always understand both formats, even with writers disabled.
type StoragePolicy struct {
	Version             int  `json:"version"`
	CompilerInputChunks bool `json:"compiler_input_chunks"`
	CheckpointPages     bool `json:"checkpoint_pages"`
	DatabaseMaxOpen     int  `json:"database_max_open"`
	DatabaseMaxIdle     int  `json:"database_max_idle"`
	DatabaseIdleSeconds int  `json:"database_idle_seconds"`
}
type storagePolicyState struct {
	mu                  sync.RWMutex
	value               StoragePolicy
	lastError           bool
	loaded              time.Time
	codec               compilerInputCodecPolicy
	codecReadFailed     bool
	codecObservedAt     time.Time
	shared              compilerInputCodecPolicy
	sharedReadFailed    bool
	sharedObservedAt    time.Time
	separated           compilerInputCodecPolicy
	separatedReadFailed bool
}

func (s *Store) StoragePolicy() StoragePolicy {
	s.storagePolicy.mu.RLock()
	defer s.storagePolicy.mu.RUnlock()
	return s.storagePolicy.value
}

func ParseStoragePolicy(raw []byte) (StoragePolicy, error) {
	var p StoragePolicy
	if len(raw) > 4096 {
		return p, errors.New("storage policy exceeds size budget")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF || p.Version != 1 || p.DatabaseMaxOpen < 0 || p.DatabaseMaxOpen > 256 || p.DatabaseMaxIdle < 0 || p.DatabaseMaxIdle > 128 || p.DatabaseIdleSeconds < 0 || p.DatabaseIdleSeconds > 3600 || p.DatabaseMaxOpen > 0 && (p.DatabaseMaxIdle > p.DatabaseMaxOpen || p.DatabaseIdleSeconds == 0) {
		return p, errors.New("invalid storage policy")
	}
	return p, nil
}

func (s *Store) RunStoragePolicy(ctx context.Context, path string) {
	if path == "" {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		s.loadCompilerInputCodecPolicy(filepath.Join(filepath.Dir(path), "compiler-input-codec.json"))
		s.loadCompilerInputSharedPolicy(filepath.Join(filepath.Dir(path), "compiler-input-shared.json"))
		s.loadCompilerInputSeparatedPolicy(filepath.Join(filepath.Dir(path), "compiler-input-separated.json"))
		f, err := os.Open(path)
		var p StoragePolicy
		if err == nil {
			var raw []byte
			raw, err = io.ReadAll(io.LimitReader(f, 4097))
			f.Close()
			if err == nil {
				p, err = ParseStoragePolicy(raw)
			}
		}
		s.storagePolicy.mu.Lock()
		s.storagePolicy.lastError = err != nil
		if err == nil && p != s.storagePolicy.value {
			if s.db != nil {
				s.db.SetMaxOpenConns(p.DatabaseMaxOpen)
				s.db.SetMaxIdleConns(p.DatabaseMaxIdle)
				s.db.SetConnMaxIdleTime(time.Duration(p.DatabaseIdleSeconds) * time.Second)
			}
			s.storagePolicy.value = p
			s.storagePolicy.loaded = time.Now().UTC()
		}
		s.storagePolicy.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Store) StorageObservation(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.storagePolicy.mu.RLock()
	codec, codecFailed, codecAt := s.storagePolicy.codec, s.storagePolicy.codecReadFailed, s.storagePolicy.codecObservedAt
	shared, sharedFailed, sharedAt := s.storagePolicy.shared, s.storagePolicy.sharedReadFailed, s.storagePolicy.sharedObservedAt
	separated, separatedFailed := s.storagePolicy.separated, s.storagePolicy.separatedReadFailed
	p, loaded, failed := s.storagePolicy.value, s.storagePolicy.loaded, s.storagePolicy.lastError
	s.storagePolicy.mu.RUnlock()
	out := map[string]any{"schema": "fugue.storage.observation.v1", "observed_at": time.Now().UTC(), "policy": p, "policy_loaded_at": loaded, "policy_read_failed": failed, "compiler_input_chunk_bytes": inputChunkBytes, "compiler_input_database_writer_limit": 1}
	out["compiler_input_codec"] = codec
	out["compiler_input_codec_read_failed"] = codecFailed
	out["compiler_input_codec_observed_at"] = codecAt
	out["compiler_input_shared"] = shared
	out["compiler_input_shared_read_failed"] = sharedFailed
	out["compiler_input_shared_observed_at"] = sharedAt
	out["compiler_input_shared_activity"] = sharedInputObservation()
	out["compiler_input_read_formats"] = []string{inputFormatV1, inputFormatCodecV2, inputFormatSharedV3, inputFormatSeparatedV4}
	out["compiler_input_separated"] = separated
	out["compiler_input_separated_read_failed"] = separatedFailed
	out["compiler_input_separated_activity"] = separatedInputObservation()
	out["compiler_input_writer_activity"] = compilerInputWriterStats.snapshot()
	out["compiler_input_writer_diagnostic"] = compilerInputWriterStats.diagnosticSnapshot()
	capabilities, err := inputhistory.BinaryCapabilities("store")
	if err != nil {
		return nil, err
	}
	out["compiler_input_history_capabilities"] = capabilities
	out["observation_retention"] = s.retentionObservation()
	if s.db != nil {
		out["database_pool"] = s.db.Stats()
	}
	return out, nil
}
