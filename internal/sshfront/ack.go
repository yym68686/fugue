package sshfront

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
)

func (service *Service) acknowledge(ctx context.Context, config Config, bundle model.EdgeSSHRouteBundle) error {
	service.mu.Lock()
	loaded := len(service.listeners) == len(bundle.Routes)
	for _, route := range bundle.Routes {
		listener, exists := service.listeners[route.PublicPort]
		if !exists || listener.route != route {
			loaded = false
		}
	}
	connections := []model.EdgeSSHRouteConnections{}
	for key, count := range service.routeConnections {
		key.Count = count
		connections = append(connections, key)
	}
	service.mu.Unlock()
	if !loaded {
		return fmt.Errorf("SSH acknowledgement refused: loaded listeners differ from the bundle")
	}
	if config.InstanceUID == "" || config.EdgeID == "" || config.EdgeGroupID == "" || config.CachePath == "" {
		return fmt.Errorf("SSH acknowledgement requires an instance, edge, group, and durable cache")
	}
	endpoint, err := url.Parse(config.APIURL)
	if err != nil {
		return err
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/v1/edge/ssh/ack"
	query := endpoint.Query()
	query.Del("token")
	endpoint.RawQuery = query.Encode()
	ack := model.EdgeSSHAcknowledgementRequest{EdgeID: config.EdgeID, EdgeGroupID: config.EdgeGroupID, InstanceUID: config.InstanceUID, BundleVersion: bundle.Version, RouteDigest: model.SSHRoutesDigest(bundle.Routes), Persisted: true, LoadedAt: time.Now().UTC()}
	ack.Connections = connections
	payload, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, config.HTTPTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(config.EdgeToken))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("SSH route acknowledgement status=%d", response.StatusCode)
	}
	var result struct {
		Accepted bool `json:"accepted"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result); err != nil {
		return err
	}
	if !result.Accepted {
		return fmt.Errorf("SSH route acknowledgement rejected")
	}
	return nil
}
