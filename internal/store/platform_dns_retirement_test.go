package store

import (
	"encoding/json"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func sealDNSRetirementTestArtifact(artifact model.PlatformArtifact, state *Store) (model.PlatformArtifact, error) {
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return artifact, err
	}
	if err = json.Unmarshal(raw, &artifact.Content); err != nil {
		return artifact, err
	}
	artifact.ContentHash, err = platformconfig.Digest(artifact.Content)
	if err != nil {
		return artifact, err
	}
	return platformsafety.SignPlatformArtifact(artifact, state.platformArtifactSigningKeyring())
}
