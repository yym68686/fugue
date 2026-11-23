package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
	"fugue/internal/routeproof"
)

// Deliberately no fence, signature, live assignment, or LKG in this input.
// Verify the rendered behaviors against the compiler's per-ingress DNS
// requirements. Keep physical ingress coverage separate from DNS admission.
type groupPreviewInput struct {
	Route                              model.PlatformArtifact
	Host                               string
	Physical, Eligible                 []pc.HostedAddressHold
	Before, Prepared, After            pc.HostedDispatchRows
	BaseProbes, HoldProbes, PathProbes []pc.DNSReadinessProbe
}

func groupPreview(r io.Reader, w io.Writer) error {
	const limit = 32 << 20
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || len(body) > limit {
		return errors.New("oversized or incomplete group preview input")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	var in groupPreviewInput
	if err := d.Decode(&in); err != nil {
		return fmt.Errorf("group preview input: %w", err)
	}
	if d.Decode(new(any)) != io.EOF || in.Host == "" || len(in.Physical) == 0 || len(in.Physical) > 256 {
		return errors.New("invalid group preview input")
	}
	all := map[pc.HostedAddressHold]bool{}
	groups := []string{}
	for _, s := range in.Physical {
		if s.EdgeID == "" || s.EdgeGroupID == "" || s.Address == "" || all[s] {
			return errors.New("invalid physical ingress")
		}
		all[s] = true
		if !slices.Contains(groups, s.EdgeGroupID) {
			groups = append(groups, s.EdgeGroupID)
		}
	}
	admit := map[pc.HostedAddressHold]bool{}
	for _, s := range in.Eligible {
		if !all[s] || admit[s] {
			return errors.New("invalid admitted ingress")
		}
		admit[s] = true
	}
	if len(admit) == 0 {
		return errors.New("empty DNS readiness subject set")
	}
	slices.Sort(groups)
	type key struct {
		target pc.HostedAddressHold
		path   string
	}
	type phase struct {
		rows     pc.HostedDispatchRows
		targets  map[pc.HostedAddressHold]bool
		probes   []pc.DNSReadinessProbe
		expected map[key]string
	}
	phases := []phase{{in.Before, all, in.BaseProbes, map[key]string{}}, {in.Prepared, admit, in.HoldProbes, map[key]string{}}, {in.After, admit, in.PathProbes, map[key]string{}}}
	for _, group := range groups {
		base, err := routeartifact.MaterializeForGroup(in.Route, group)
		if err != nil {
			return err
		}
		parts := func(c routeartifact.HostedGroupContent) model.EdgeRouteBundle {
			return model.EdgeRouteBundle{EdgeGroupID: group, Routes: c.Routes, TLSAllowlist: c.TLSAllowlist, CachePolicies: c.CachePolicies}
		}
		hold, err := routeartifact.PreviewHostedGroupRows(base, in.Host, in.Before, in.Prepared)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		after, err := routeartifact.PreviewHostedGroupRows(parts(hold), in.Host, in.Prepared, in.After)
		if err != nil {
			return fmt.Errorf("forward: %w", err)
		}
		back, err := routeartifact.PreviewHostedGroupRows(parts(after), in.Host, in.After, in.Prepared)
		if err != nil || !reflect.DeepEqual(back, hold) {
			return errors.Join(errors.New("withdrawal changed prepared group content"), err)
		}
		for i, b := range []model.EdgeRouteBundle{base, parts(hold), parts(after)} {
			for _, route := range b.Routes {
				if route.Hostname == in.Host {
					digest, err := routeproof.Digest(route)
					if err != nil {
						return err
					}
					for s := range phases[i].targets {
						if s.EdgeGroupID == group {
							phases[i].expected[key{s, model.NormalizeAppRoutePathPrefix(route.PathPrefix)}] = digest
						}
					}
				}
			}
		}
	}
	for _, p := range phases {
		if len(p.expected) != len(p.targets)*len(p.rows.Routes) || len(p.expected) != len(p.probes) {
			return errors.New("incomplete ingress/path closure")
		}
		for _, pr := range p.probes {
			k := key{pc.HostedAddressHold{EdgeID: pr.EdgeID, EdgeGroupID: pr.EdgeGroupID, Address: pr.Address}, pr.Path}
			if pr.Hostname != in.Host || pr.RouteDigest == "" || p.expected[k] != pr.RouteDigest {
				return errors.New("DNS requirement differs from rendered group route")
			}
			delete(p.expected, k)
		}
	}
	return json.NewEncoder(w).Encode(map[string]any{"schema": "hosted-group-content-preview/v1", "route_artifact": in.Route.ID, "physical": len(all), "dns_eligible": len(admit), "groups": len(groups), "paths": len(in.After.Routes), "matched_probes": []int{len(in.BaseProbes), len(in.HoldProbes), len(in.PathProbes)}, "withdraw_restores_prepared_content": true, "authorizes_traffic": false})
}
