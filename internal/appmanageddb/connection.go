package appmanageddb

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type Connection struct {
	ServiceID string
	DSN       string
	Host      string
	Port      string
	Database  string
	User      string
}

type BoundService struct {
	Binding model.ServiceBinding
	Service model.BackingService
}

func BoundServices(app model.App) []BoundService {
	servicesByID := make(map[string]model.BackingService, len(app.BackingServices))
	for _, service := range app.BackingServices {
		servicesByID[service.ID] = service
	}

	bound := make([]BoundService, 0, len(app.Bindings))
	for _, binding := range app.Bindings {
		service, ok := servicesByID[binding.ServiceID]
		if !ok {
			continue
		}
		bound = append(bound, BoundService{
			Binding: binding,
			Service: service,
		})
	}

	return bound
}

func FirstPostgresBinding(app model.App) (BoundService, bool) {
	var fallback BoundService
	for _, bound := range BoundServices(app) {
		if !strings.EqualFold(strings.TrimSpace(bound.Service.Type), model.BackingServiceTypePostgres) {
			continue
		}
		if !IsManagedService(bound.Service) {
			continue
		}
		if bound.Service.OwnerAppID == app.ID {
			return bound, true
		}
		if fallback.Service.ID == "" {
			fallback = bound
		}
	}
	if fallback.Service.ID != "" {
		return fallback, true
	}
	return BoundService{}, false
}

func IsManagedService(service model.BackingService) bool {
	provisioner := strings.TrimSpace(service.Provisioner)
	return provisioner == "" || strings.EqualFold(provisioner, model.BackingServiceProvisionerManaged)
}

var postgresServiceLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// Resolve only from the platform's recorded binding. App and binding environment
// overrides are for the workload and must never configure a control-plane connection.
func Resolve(app model.App) (Connection, error) {
	bound, ok := FirstPostgresBinding(app)
	service := bound.Service
	if !ok || app.TenantID == "" || service.TenantID != app.TenantID || bound.Binding.TenantID != app.TenantID ||
		bound.Binding.AppID != app.ID || bound.Binding.ServiceID != service.ID || service.ID == "" || service.Spec.Postgres == nil {
		return Connection{}, errors.New("an authorized managed database binding is required")
	}
	pg := *service.Spec.Postgres
	name := model.PostgresEndpointName(pg)
	if !postgresServiceLabel.MatchString(name) || pg.Database == "" || pg.User == "" || pg.Password == "" || pg.Suspended {
		return Connection{}, errors.New("managed database connection unavailable")
	}
	host := name + "." + runtime.NamespaceForTenant(service.TenantID) + ".svc.cluster.local"
	u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(host, "5432"), Path: "/" + pg.Database, User: url.UserPassword(pg.User, pg.Password)}
	return Connection{ServiceID: service.ID, DSN: u.String(), Host: host, Port: "5432", Database: pg.Database, User: pg.User}, nil
}
