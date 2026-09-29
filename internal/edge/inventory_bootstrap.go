package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

// An operator-projected bootstrap permission enrolls one private executor's
// first inventory epoch. It has no bundle, health, public transport or LKG claim.
type inventoryBootstrapAuthorization struct {
	Schema           string    `json:"schema"`
	AuthorityCellID  string    `json:"authority_cell_id"`
	EdgeID           string    `json:"edge_id"`
	InstanceUID      string    `json:"instance_uid"`
	Slot             string    `json:"slot"`
	SourceSHA        string    `json:"source_sha"`
	ReleaseSetID     string    `json:"release_set_id"`
	ReleaseSetDigest string    `json:"release_set_digest"`
	ReleaseID        string    `json:"release_id"`
	IssuedAt         time.Time `json:"issued_at"`
	ExpiresAt        time.Time `json:"expires_at"`
}

type inventoryEpochSelection struct {
	Slot       string
	Fence      uint64
	Activation edgegroupfront.ActivationState
	Bootstrap  *inventoryBootstrapAuthorization
}

func (s *Service) selectInventoryEpoch(ctx context.Context, cfg config.EdgeConfig) (inventoryEpochSelection, error) {
	activation, exists, err := edgegroupfront.ReadActivationState(s.InventoryProducer.ActivationStateFile)
	if err == nil && exists {
		if activation.GroupID != cfg.EdgeGroupID {
			return inventoryEpochSelection{}, errors.New("Edge inventory producer activation group is invalid")
		}
		return inventoryEpochSelection{Slot: activation.ActiveSlot, Fence: activation.Generation, Activation: activation}, nil
	}
	// Corrupt, unreadable and mismatched activation never permit bootstrap.
	if !errors.Is(err, os.ErrNotExist) || s.InventoryProducer.BootstrapFile == "" {
		return inventoryEpochSelection{}, errors.New("Edge inventory producer activation state is unavailable")
	}
	raw, err := platformconsumer.ReadFile(s.InventoryProducer.BootstrapFile, 16<<10)
	if err != nil {
		return inventoryEpochSelection{}, errors.New("initial cell bootstrap authorization unavailable")
	}
	a, err := decodeInventoryBootstrap(raw, cfg, time.Now().UTC())
	if err != nil {
		return inventoryEpochSelection{}, err
	}
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient, AuthorityID: a.AuthorityCellID}
	id, assignment, route, release, err := client.SyncServing(ctx, model.PlatformConsumerComponentEdgeWorker, cfg.EdgeID, cfg.PlatformScopeKey, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return inventoryEpochSelection{}, err
	}
	if assignment.ReleaseSetID != a.ReleaseSetID || release.ID != a.ReleaseID || release.ReleaseChannel != model.PlatformArtifactReleaseChannelGray {
		return inventoryEpochSelection{}, errors.New("bootstrap permission is not bound to the selected gray publication")
	}
	parent, err := client.ReleaseSet(ctx, id, assignment, release)
	if err != nil {
		return inventoryEpochSelection{}, err
	}
	keys := bundleauth.NewKeyring(cfg.BundleSigningKey, cfg.BundleSigningKeyID, cfg.BundleSigningPreviousKey, cfg.BundleSigningPreviousKeyID, cfg.BundleRevokedKeyIDs)
	if _, err = routeartifact.ProjectRelease(parent, route, assignment, release, keys); err != nil {
		return inventoryEpochSelection{}, err
	}
	topology, err := platformconfig.TrafficConsumersFromRelease(parent)
	if err != nil || topology == nil || topology.PublicationRole != platformconfig.PublicationRoleCellRoutes || topology.AuthorityCellID != a.AuthorityCellID || !slices.Contains(topology.EdgeNodeIDs, cfg.EdgeID) || parent.ContentHash != a.ReleaseSetDigest {
		return inventoryEpochSelection{}, errors.New("bootstrap permission differs from signed cell route membership")
	}
	if err = client.CheckServingAssignment(ctx, id, assignment); err != nil {
		return inventoryEpochSelection{}, err
	}
	if !a.ExpiresAt.After(time.Now().UTC()) {
		return inventoryEpochSelection{}, errors.New("initial cell bootstrap authorization expired")
	}
	return inventoryEpochSelection{Slot: a.Slot, Fence: 1, Bootstrap: &a}, nil
}

func decodeInventoryBootstrap(raw []byte, cfg config.EdgeConfig, now time.Time) (inventoryBootstrapAuthorization, error) {
	var a inventoryBootstrapAuthorization
	if !uniqueBootstrapFields(raw) {
		return a, errors.New("initial cell bootstrap fields are ambiguous")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&a) != nil || d.Decode(&struct{}{}) != io.EOF {
		return a, errors.New("initial cell bootstrap authorization invalid")
	}
	cell := platformcontrol.ConsumerAuthorityID(cfg.EdgeGroupID)
	if a.Schema != "fugue.cell-inventory-bootstrap/v1" || cell == "" || a.AuthorityCellID != cell || cfg.PlatformScopeKey != platformconfig.AuthorityCellScope(cell) || a.EdgeID != cfg.EdgeID || a.InstanceUID != cfg.EdgeInstanceUID || a.Slot != cfg.EdgeSlot || a.SourceSHA != cfg.EdgeReleaseEpoch || a.EdgeID == "" || a.InstanceUID == "" || a.Slot != "a" && a.Slot != "b" || len(a.SourceSHA) != 40 || strings.Trim(a.SourceSHA, "0123456789abcdef") != "" || a.ReleaseSetID == "" || a.ReleaseID == "" || len(a.ReleaseSetID) > 256 || len(a.ReleaseID) > 256 || len(a.ReleaseSetDigest) != 71 || !strings.HasPrefix(a.ReleaseSetDigest, "sha256:") || strings.Trim(a.ReleaseSetDigest[7:], "0123456789abcdef") != "" {
		return a, errors.New("initial cell bootstrap identity or publication differs")
	}
	if a.IssuedAt.IsZero() || a.IssuedAt.After(now.Add(30*time.Second)) || !a.ExpiresAt.After(now) || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > 15*time.Minute {
		return a, errors.New("initial cell bootstrap authorization outside its absolute lease")
	}
	return a, nil
}

func uniqueBootstrapFields(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for d.More() {
		token, err = d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return false
		}
	}
	_, err = d.Token()
	return err == nil && d.Decode(&struct{}{}) == io.EOF
}
