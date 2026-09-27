package agentedge

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"time"

	"fugue/internal/staticedgecontract"
)

const checkpointSchema = "fugue.agent-edge-checkpoint/v1"
const maxCheckpointBytes = 1 << 20

func checkpointMarker(path, audience, origin string, checkpointFound bool) error {
	marker := path + ".initialized"
	expected, _ := json.Marshal(struct{ Schema, Audience, Origin string }{"fugue.agent-edge-checkpoint-owner/v1", audience, origin})
	f, err := os.Open(marker)
	if err == nil {
		defer f.Close()
		info, statErr := f.Stat()
		raw, readErr := io.ReadAll(io.LimitReader(f, 4097))
		if !checkpointFound || statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || readErr != nil || !bytes.Equal(raw, expected) {
			return errors.New("Agent Edge initialized checkpoint is missing or its owner marker is invalid")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return errors.New("Agent Edge checkpoint owner marker unavailable")
	}
	f, err = os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("Agent Edge checkpoint owner marker could not be created")
	}
	if _, err = f.Write(expected); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.New("Agent Edge checkpoint owner marker could not be persisted")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("Agent Edge checkpoint owner directory unavailable")
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return errors.New("Agent Edge checkpoint owner directory synchronization failed")
	}
	return nil
}

// This is local restrictive state, never a source of new authorization.
// Cached permissions still require a live signature and fresh local probes.
// Keeping all cell watermarks prevents a temporarily absent cell from replaying
// an older publication after the Agent restarts.
type clientCheckpoint struct {
	Schema    string                 `json:"schema"`
	Audience  string                 `json:"audience"`
	Origin    string                 `json:"origin"`
	Activated bool                   `json:"activated"`
	Trust     TrustKeyring           `json:"trust"`
	LastGrant *SignedGrant           `json:"last_grant,omitempty"`
	Cells     map[string]Publication `json:"cell_watermarks"`
}

func (c clientCheckpoint) validate(audience, origin string) error {
	if c.Schema != checkpointSchema || c.Audience != audience || c.Origin != origin || !identifier.MatchString(audience) || len(c.Cells) > 256 {
		return errors.New("Agent Edge checkpoint identity or bounds are invalid")
	}
	if _, err := c.Trust.PublicKeys(); err != nil {
		return err
	}
	if c.LastGrant == nil {
		if c.Activated || len(c.Cells) != 0 {
			return errors.New("Agent Edge checkpoint lost its accepted grant")
		}
		return nil
	}
	s := c.LastGrant
	signature, err := base64.RawURLEncoding.DecodeString(s.Signature)
	if s.Grant.Validate() != nil || s.Grant.Audience != audience || s.Grant.Origin != origin || !identifier.MatchString(s.KeyID) || s.Digest != grantDigest(s.Grant) || err != nil || len(signature) != 64 || s.Grant.Mode == "active" && !c.Activated {
		return errors.New("Agent Edge checkpoint grant is invalid")
	}
	for cell, publication := range c.Cells {
		if !topologyIdentifier.MatchString(cell) || len(cell) > 128 || publication.validate(s.Grant.IssuedAt) != nil {
			return errors.New("Agent Edge checkpoint publication floor is invalid")
		}
	}
	for _, candidate := range s.Grant.Candidates {
		if c.Cells[candidate.AuthorityCellID] != candidate.Publication {
			return errors.New("Agent Edge checkpoint lost a publication floor")
		}
	}
	return nil
}

func readCheckpoint(path, audience, origin string) (clientCheckpoint, bool, error) {
	var c clientCheckpoint
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return c, false, nil
	}
	if err != nil {
		return c, false, errors.New("Agent Edge checkpoint unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxCheckpointBytes || info.Mode().Perm()&0022 != 0 {
		return c, false, errors.New("Agent Edge checkpoint permissions or size invalid")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxCheckpointBytes+1))
	if err != nil || len(raw) > maxCheckpointBytes || staticedgecontract.StrictJSON(raw, &c) != nil || c.validate(audience, origin) != nil {
		return c, false, errors.New("Agent Edge checkpoint is corrupt or foreign")
	}
	return c, true, nil
}

func writeCheckpoint(path string, c clientCheckpoint) error {
	if !filepath.IsAbs(path) || c.validate(c.Audience, c.Origin) != nil {
		return errors.New("Agent Edge checkpoint target or content invalid")
	}
	raw, err := json.Marshal(c)
	if err != nil || len(raw) > maxCheckpointBytes {
		return errors.New("Agent Edge checkpoint exceeds bounds")
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".agent-edge-checkpoint-")
	if err != nil {
		return errors.New("Agent Edge checkpoint temporary file unavailable")
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closed := f.Close()
	if err != nil || closed != nil {
		return errors.New("Agent Edge checkpoint write failed")
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return errors.New("Agent Edge checkpoint activation failed")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("Agent Edge checkpoint directory unavailable")
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return errors.New("Agent Edge checkpoint directory synchronization failed")
	}
	return nil
}

func (s *Selector) restoreCheckpoint(c clientCheckpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.LastGrant != nil {
		signed := *c.LastGrant
		signed.Grant = cloneGrant(signed.Grant)
		s.grant = VerifiedGrant{signed: signed, cellWatermarks: maps.Clone(c.Cells)}
	}
	// Never restore elapsed measurements, cooldown counters or connection pools.
	s.observations = nil
	s.lastRound = time.Time{}
	s.primary = ""
	s.challenger = ""
	s.betterRounds = 0
}

func (s *Selector) permission(keys map[string]TrustKey, now time.Time) (VerifiedGrant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.grant.Live(keys, now) {
		return VerifiedGrant{}, errors.New("no live Agent Edge permission")
	}
	signed := s.grant.signed
	signed.Grant = cloneGrant(signed.Grant)
	return VerifiedGrant{signed: signed, cellWatermarks: maps.Clone(s.grant.cellWatermarks)}, nil
}
