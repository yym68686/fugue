package api

import (
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func (s *Server) bundleKeyring() bundleauth.Keyring {
	if s == nil {
		return bundleauth.Keyring{}
	}
	return bundleauth.NewKeyring(
		s.bundleSigningKey,
		s.bundleSigningKeyID,
		s.bundleSigningPreviousKey,
		s.bundleSigningPreviousKeyID,
		s.bundleRevokedKeyIDs,
	)
}

func signDiscoveryBundle(bundle model.DiscoveryBundle, keyring bundleauth.Keyring, validFor time.Duration) model.DiscoveryBundle {
	return bundleauth.SignDiscoveryBundleWithKeyring(bundle, keyring, validFor)
}

func signEdgeRouteBundle(bundle model.EdgeRouteBundle, keyring bundleauth.Keyring, validFor time.Duration) model.EdgeRouteBundle {
	return bundleauth.SignEdgeRouteBundleWithKeyring(bundle, keyring, validFor)
}

func signEdgeDNSBundle(bundle model.EdgeDNSBundle, keyring bundleauth.Keyring, validFor time.Duration) model.EdgeDNSBundle {
	return bundleauth.SignEdgeDNSBundleWithKeyring(bundle, keyring, validFor)
}
