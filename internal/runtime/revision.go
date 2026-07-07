package runtime

import (
	"strconv"
	"strings"

	"fugue/internal/model"
)

const (
	AppRevisionRoleDefault   = "default"
	AppRevisionRoleStable    = "stable"
	AppRevisionRoleCandidate = "candidate"
	AppRevisionRolePrevious  = "previous"
)

type AppRevisionRenderOptions struct {
	Role      string
	ReleaseID string
	Suffix    string
}

func NormalizeAppRevisionRenderOptions(options AppRevisionRenderOptions) AppRevisionRenderOptions {
	options.Role = strings.TrimSpace(strings.ToLower(options.Role))
	switch options.Role {
	case "", AppRevisionRoleDefault:
		options.Role = AppRevisionRoleDefault
	case AppRevisionRoleStable, AppRevisionRoleCandidate, AppRevisionRolePrevious:
	default:
		options.Role = AppRevisionRoleDefault
	}
	options.ReleaseID = strings.TrimSpace(options.ReleaseID)
	options.Suffix = strings.TrimSpace(options.Suffix)
	if options.Role != AppRevisionRoleDefault && options.Suffix == "" {
		options.Suffix = options.Role
	}
	options.Suffix = model.SlugifyOptional(strings.ReplaceAll(options.Suffix, "_", "-"))
	return options
}

func RuntimeAppResourceNameWithOptions(app model.App, options RenderOptions) string {
	base := RuntimeAppResourceName(app)
	revision := normalizeRenderOptions(options).Revision
	if revision.Role == AppRevisionRoleDefault || revision.Suffix == "" {
		return base
	}
	return model.DNS1035Label(base+"-"+revision.Suffix, base)
}

func RuntimeAppServiceNameWithOptions(app model.App, options RenderOptions) string {
	return RuntimeServiceName(RuntimeAppResourceNameWithOptions(app, options))
}

func AppRevisionServiceURL(app model.App, options RenderOptions) string {
	port := 80
	if app.Route != nil && app.Route.ServicePort > 0 {
		port = app.Route.ServicePort
	} else if len(app.Spec.Ports) > 0 && app.Spec.Ports[0] > 0 {
		port = app.Spec.Ports[0]
	}
	return "http://" + RuntimeAppServiceNameWithOptions(app, options) + "." + NamespaceForTenant(app.TenantID) + ".svc.cluster.local:" + strconv.Itoa(port)
}

func appRevisionLabels(options RenderOptions) map[string]string {
	revision := normalizeRenderOptions(options).Revision
	if revision.Role == AppRevisionRoleDefault {
		return nil
	}
	labels := map[string]string{
		FugueLabelAppReleaseRole: revision.Role,
	}
	if revision.ReleaseID != "" {
		labels[FugueLabelAppReleaseID] = revision.ReleaseID
	}
	return labels
}

func (r Renderer) BuildManagedAppRevisionChildObjects(app model.App, scheduling SchedulingConstraints, postgresPlacements map[string][]SchedulingConstraints, ownerRef *OwnerReference, revision AppRevisionRenderOptions) []map[string]any {
	options := r.renderOptions()
	options.Revision = revision
	return BuildManagedAppChildObjectsWithPlacementsAndOptions(r.PrepareApp(app), scheduling, postgresPlacements, ownerRef, options)
}

func (r Renderer) AppRevisionServiceURL(app model.App, revision AppRevisionRenderOptions) string {
	options := r.renderOptions()
	options.Revision = revision
	return AppRevisionServiceURL(r.PrepareApp(app), options)
}
