// Package composition is the default-disabled composition root for the
// backup observer input-bundle read path. It joins already separated
// capabilities without registering a route, starting a server, or owning a
// Kubernetes or datastore mutation capability.
package composition

import (
	"errors"
	"net/http"
	"time"

	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer/httpapi"
	"fugue/internal/backupmaterializer/legacysource"
	"fugue/internal/backupmaterializer/localissuer"
	"fugue/internal/backupmaterializer/storesource"
	"fugue/internal/backupmaterializeridentity/httpauth"
	"fugue/internal/backupmaterializerreview/projected"
)

var ErrConfig = errors.New("backup materializer composition configuration invalid")

// Config contains the complete capability set for one private materializer
// read boundary. Disabled configurations intentionally ignore every other
// field, so the default state cannot touch the store, projection, clock, or
// signing material.
type Config struct {
	Enabled         bool
	Store           storesource.ReadStore
	ObserverKeyring backupidentity.Keyring
	TokenReview     projected.Config
	Now             func() time.Time
}

func (config Config) String() string {
	return "backup materializer composition configuration [REDACTED]"
}

func (config Config) GoString() string {
	return config.String()
}

// Handler is route-agnostic. A later server-wiring atom must explicitly bind
// it to the generated route; until then this package remains unreachable.
type Handler struct {
	enabled bool
	next    http.Handler
}

// BackupMaterializerEndpointV1 is a compile-time marker required by the
// generated API route gate. It prevents an arbitrary http.Handler from being
// injected as this private identity-owning endpoint by accident.
func (handler *Handler) BackupMaterializerEndpointV1() {}

func New(config Config) (*Handler, error) {
	if !config.Enabled {
		return &Handler{}, nil
	}
	if config.Now == nil {
		return nil, ErrConfig
	}
	reader, err := storesource.New(config.Store)
	if err != nil {
		return nil, ErrConfig
	}
	source, err := legacysource.New(reader.ReadSnapshot)
	if err != nil {
		return nil, ErrConfig
	}
	issuer, err := localissuer.New(config.ObserverKeyring)
	if err != nil {
		return nil, ErrConfig
	}
	reviewer, err := projected.New(config.TokenReview)
	if err != nil {
		return nil, ErrConfig
	}
	authenticator, err := httpauth.New(reviewer, config.Now)
	if err != nil {
		return nil, ErrConfig
	}
	core, err := httpapi.New(source, issuer, config.Now)
	if err != nil {
		return nil, ErrConfig
	}
	return &Handler{enabled: true, next: authenticator.RequireGET(core)}, nil
}

func (handler *Handler) Enabled() bool {
	return handler != nil && handler.enabled && handler.next != nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setPrivateHeaders(writer.Header())
	if !handler.Enabled() || request == nil {
		http.NotFound(writer, request)
		return
	}
	handler.next.ServeHTTP(writer, request)
}

func (handler *Handler) String() string {
	return "backup materializer composition handler [REDACTED]"
}

func (handler *Handler) GoString() string {
	return handler.String()
}

func setPrivateHeaders(header http.Header) {
	header.Set("Cache-Control", "private, no-store, max-age=0")
	header.Set("Pragma", "no-cache")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Vary", "Authorization")
}
