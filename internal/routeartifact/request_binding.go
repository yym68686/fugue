package routeartifact

import (
	"crypto/sha256"
	"encoding/json"
	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/trafficbinding"
)

// BindingReader validates the complete route projection and returns its binding.
// It does not resolve a current release, expected topology or serving authority.
type BindingReader func(model.PlatformArtifact, model.PlatformArtifact, model.PlatformConsumerAssignment, model.PlatformArtifactRelease, bundleauth.Keyring) (*model.TrafficReleaseBinding, error)

// NewRequestBindingReader is owned by one sequential request. Callers still read
// mutable release selection and topology before and after observing artifacts.
// It must not be retained across requests or used to skip those authority reads.
func NewRequestBindingReader() BindingReader {
	return newRequestBindingReader(ProjectRelease)
}

func newRequestBindingReader(project func(model.PlatformArtifact, model.PlatformArtifact, model.PlatformConsumerAssignment, model.PlatformArtifactRelease, bundleauth.Keyring) (model.EdgeRouteIntentSnapshot, error)) BindingReader {
	// Only one small binding is retained, never a full projected route table.
	// Fingerprint complete inputs on every call, including actual content bytes
	// and current signing keys; neither claimed hashes nor pointer identity can
	// conceal mutation or revocation. Failed computations are never retained.
	var previous [32]byte
	var binding *model.TrafficReleaseBinding
	var havePrevious bool
	return func(parent, child model.PlatformArtifact, assignment model.PlatformConsumerAssignment, release model.PlatformArtifactRelease, keys bundleauth.Keyring) (*model.TrafficReleaseBinding, error) {
		key, supported := requestBindingKey(parent, child, assignment, release, keys)
		if supported && havePrevious && key == previous {
			return trafficbinding.Clone(binding), nil
		}
		snapshot, err := project(parent, child, assignment, release, keys)
		if err == nil && supported {
			previous, binding, havePrevious = key, trafficbinding.Clone(snapshot.TrafficRelease), true
		}
		return trafficbinding.Clone(snapshot.TrafficRelease), err
	}
}

func requestBindingKey(parent, child model.PlatformArtifact, assignment model.PlatformConsumerAssignment, release model.PlatformArtifactRelease, keys bundleauth.Keyring) ([32]byte, bool) {
	parentContent, parentOK := platformsafety.FingerprintArtifactContent(parent.Content)
	childContent, childOK := platformsafety.FingerprintArtifactContent(child.Content)
	if !parentOK || !childOK {
		return [32]byte{}, false
	}
	// Struct copies preserve all metadata in the key without serializing the
	// large bodies again. Typed tree fingerprints distinguish values that encode
	// to identical JSON but may differ under validation's Go type assertions.
	parent.Content, child.Content = nil, nil
	raw, err := json.Marshal(struct {
		ParentContent [32]byte
		ChildContent  [32]byte
		Parent        model.PlatformArtifact
		Child         model.PlatformArtifact
		Assignment    model.PlatformConsumerAssignment
		Release       model.PlatformArtifactRelease
		Keys          bundleauth.Keyring
	}{parentContent, childContent, parent, child, assignment, release, keys})
	if err != nil {
		return [32]byte{}, false
	}
	return sha256.Sum256(raw), true
}
