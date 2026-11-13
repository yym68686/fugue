package store

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

type compilerInputBlockPolicy struct {
	Version      int `json:"version"`
	MinimumBytes int `json:"minimum_bytes"`
	BoundaryBits int `json:"boundary_bits,omitempty"`
}

func parseCompilerInputBlockPolicy(raw []byte) (compilerInputBlockPolicy, error) {
	var p compilerInputBlockPolicy
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if len(raw) > 4096 || d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF || p.Version != 1 {
		return p, ErrInvalidInput
	}
	if p.BoundaryBits != 0 && p.BoundaryBits != 3 && p.BoundaryBits != 4 {
		return p, ErrInvalidInput
	}
	switch p.MinimumBytes {
	case 256, 2048, 4096, 8192:
		return p, nil
	}
	return p, ErrInvalidInput
}

func (s *Store) compilerInputBlockMinimum() int {
	s.storagePolicy.mu.RLock()
	defer s.storagePolicy.mu.RUnlock()
	if s.storagePolicy.blocks.Version != 1 {
		return sharedBlockMin
	}
	return s.storagePolicy.blocks.MinimumBytes
}

func (s *Store) compilerInputBoundaryBits() int {
	s.storagePolicy.mu.RLock()
	defer s.storagePolicy.mu.RUnlock()
	if s.storagePolicy.blocks.Version != 1 {
		return 0
	}
	return s.storagePolicy.blocks.BoundaryBits
}

func (s *Store) loadCompilerInputBlockPolicy(path string) {
	f, err := os.Open(path)
	missing := os.IsNotExist(err)
	var p compilerInputBlockPolicy
	if err == nil {
		var raw []byte
		raw, err = io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		if err == nil {
			p, err = parseCompilerInputBlockPolicy(raw)
		}
	}
	if err != nil {
		p = compilerInputBlockPolicy{}
	}
	s.storagePolicy.mu.Lock()
	defer s.storagePolicy.mu.Unlock()
	s.storagePolicy.blocks = p
	s.storagePolicy.blocksReadFailed = err != nil && !missing
}
