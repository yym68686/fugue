package apispec

//go:generate go run ../../cmd/fugue-openapi-gen -spec ../../openapi/openapi.yaml -routes-out ../api/routes_gen.go -spec-out ./spec_gen.go

type AuthKind string

const (
	AuthNone        AuthKind = "none"
	AuthAPI         AuthKind = "api"
	AuthRuntime     AuthKind = "runtime"
	AuthNodeUpdater AuthKind = "node-updater"
)

type Route struct {
	Method      string
	Path        string
	Pattern     string
	OperationID string
	HandlerName string
	Auth        AuthKind
}

func YAML() []byte {
	return append([]byte(nil), openAPISpecYAML...)
}

func JSON() []byte {
	return append([]byte(nil), openAPISpecJSON...)
}

func Routes() []Route {
	out := make([]Route, len(openAPIRoutes))
	copy(out, openAPIRoutes)
	return out
}
