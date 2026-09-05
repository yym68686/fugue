package model

// DeepCopy returns a detached, semantically identical AppSpec. It performs no
// normalization, defaulting, or runtime-env stripping; callers apply those
// policies explicitly after copying.
func (spec AppSpec) DeepCopy() AppSpec {
	out := spec
	out.Command = append([]string(nil), spec.Command...)
	out.Args = append([]string(nil), spec.Args...)
	out.Ports = append([]int(nil), spec.Ports...)
	out.Env = cloneStringMap(spec.Env)
	out.GeneratedEnv = cloneGeneratedEnv(spec.GeneratedEnv)
	if spec.SSH != nil {
		v := *spec.SSH
		v.AuthorizedKeyIDs = append([]string(nil), spec.SSH.AuthorizedKeyIDs...)
		v.AuthorizedKeys = append([]string(nil), spec.SSH.AuthorizedKeys...)
		out.SSH = &v
	}
	out.NetworkPolicy = cloneNetworkPolicy(spec.NetworkPolicy)
	out.Resources = CloneResourceSpec(spec.Resources)
	if spec.RightSizing != nil {
		v := *spec.RightSizing
		out.RightSizing = &v
	}
	out.Files = append([]AppFile(nil), spec.Files...)
	if spec.Workspace != nil {
		v := *spec.Workspace
		out.Workspace = &v
	}
	if spec.Data != nil {
		v := *spec.Data
		v.Workspaces = cloneDataWorkspaces(spec.Data.Workspaces)
		if spec.Data.EgressEstimate != nil {
			e := *spec.Data.EgressEstimate
			v.EgressEstimate = &e
		}
		out.Data = &v
	}
	if spec.PersistentStorage != nil {
		v := *spec.PersistentStorage
		v.Mounts = append([]AppPersistentStorageMount(nil), spec.PersistentStorage.Mounts...)
		out.PersistentStorage = &v
	}
	if spec.VolumeReplication != nil {
		v := *spec.VolumeReplication
		out.VolumeReplication = &v
	}
	if spec.Postgres != nil {
		out.Postgres = CloneAppPostgresSpec(spec.Postgres)
	}
	if spec.Failover != nil {
		v := *spec.Failover
		out.Failover = &v
	}
	out.Continuity = CloneAppContinuityPolicy(spec.Continuity)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func cloneGeneratedEnv(in map[string]AppGeneratedEnvSpec) map[string]AppGeneratedEnvSpec {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]AppGeneratedEnvSpec, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func cloneNetworkPolicy(in *AppNetworkPolicySpec) *AppNetworkPolicySpec {
	if in == nil {
		return nil
	}
	out := *in
	out.Egress = cloneNetworkDirection(in.Egress)
	out.Ingress = cloneNetworkDirection(in.Ingress)
	return &out
}
func cloneNetworkDirection(in *AppNetworkPolicyDirectionSpec) *AppNetworkPolicyDirectionSpec {
	if in == nil {
		return nil
	}
	out := *in
	out.AllowApps = make([]AppNetworkPolicyAppPeer, len(in.AllowApps))
	for i, p := range in.AllowApps {
		out.AllowApps[i] = p
		out.AllowApps[i].Ports = append([]int(nil), p.Ports...)
	}
	return &out
}
func cloneDataWorkspaces(in []AppDataWorkspaceMaterialization) []AppDataWorkspaceMaterialization {
	if len(in) == 0 {
		return nil
	}
	out := append([]AppDataWorkspaceMaterialization(nil), in...)
	for i := range out {
		out[i].Assets = append([]string(nil), in[i].Assets...)
	}
	return out
}
