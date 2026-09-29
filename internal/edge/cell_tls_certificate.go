package edge

import (
	"context"
	"errors"
	"net/url"
	"slices"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

func (s *Service) cellCertificateIdentityConfigured() bool {
	return platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID) != ""
}

func (s *Service) fetchCellTLSCertificate(ctx context.Context, hostname string) (*caddyTLSCertificateBundle, error) {
	cell := platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID)
	if cell == "" || s.Config.PlatformScopeKey != platformconfig.AuthorityCellScope(cell) {
		return nil, errors.New("cell TLS certificate scope differs")
	}
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient, AuthorityID: cell}
	id, ta, artifact, tr, err := client.SyncServing(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, s.Config.PlatformScopeKey, model.PlatformArtifactKindCaddyRouteConfig)
	if err != nil {
		return nil, err
	}
	_, ra, route, rr, err := client.SyncServing(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, s.Config.PlatformScopeKey, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return nil, err
	}
	parent, err := client.ReleaseSet(ctx, id, ra, rr)
	if err != nil {
		return nil, err
	}
	projection, err := routeartifact.ProjectRelease(parent, route, ra, rr, bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs))
	if err != nil {
		return nil, err
	}
	var ids []string
	if raw, ok := parent.Content["artifact_ids"].([]any); ok {
		for _, item := range raw {
			id, _ := item.(string)
			ids = append(ids, id)
		}
	}
	if !slices.Contains(ids, artifact.ID) {
		return nil, errors.New("TLS certificate artifact is not a member of the signed parent")
	}
	tls := edgePlatformCandidate{Artifact: artifact, Assignment: ta, Release: tr}
	routes := edgePlatformCandidate{Artifact: route, Assignment: ra, Release: rr}
	payload, err := s.verifyPlatformTLSCandidate(tls, routes)
	if err != nil {
		return nil, err
	}
	bundle, err := routeartifact.MaterializeSnapshotForGroup(projection, cell)
	if err != nil {
		return nil, err
	}
	var ref *platformconfig.TLSIntent
	for i := range payload.Certificates {
		candidate := &payload.Certificates[i]
		if candidate.Hostname == hostname {
			if ref != nil {
				return nil, errors.New("cell certificate reference is ambiguous")
			}
			ref = candidate
		}
	}
	if ref == nil || ref.Policy != model.EdgeRouteTLSPolicyCustomDomain || ref.AppID == "" || ref.TenantID == "" || !s.tlsReadinessHostAuthorized(*ref, bundle.Routes, bundle) {
		return nil, errors.New("cell certificate hostname is not authorized by signed local routes")
	}
	var response struct {
		Certificate model.EdgeTLSCertificate `json:"certificate"`
	}
	path := "/v1/platform-state/consumers/artifacts/" + url.PathEscape(artifact.ID) + "/tls/" + url.PathEscape(hostname) + "?expected_consumer_set_id=" + url.QueryEscape(ta.ExpectedConsumerSetID)
	if err := client.GetJSON(ctx, path, id.Token, &response); err != nil {
		return nil, err
	}
	cert := response.Certificate
	if cert.Hostname != hostname || cert.AppID != ref.AppID || cert.TenantID != ref.TenantID || cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, errors.New("cell certificate response belongs to a different owner or is incomplete")
	}
	if err := client.CheckServingAssignment(ctx, id, ta); err != nil {
		return nil, err
	}
	if err := client.CheckServingAssignment(ctx, id, ra); err != nil {
		return nil, err
	}
	// The existing installer validates hostname, key pair and certificate
	// validity, writes private files, and Caddy independently proves readiness.
	return &caddyTLSCertificateBundle{CertificatePEM: cert.CertificatePEM, PrivateKeyPEM: cert.PrivateKeyPEM, MetadataJSON: cert.MetadataJSON, IssuerStorage: cert.IssuerStorage}, nil
}
