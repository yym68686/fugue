package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

type contextKey string

const principalContextKey contextKey = "principal"

type Authenticator struct {
	Store             *store.Store
	BootstrapAdminKey string
}

func New(store *store.Store, bootstrapAdminKey string) *Authenticator {
	return &Authenticator{
		Store:             store,
		BootstrapAdminKey: bootstrapAdminKey,
	}
}

func (a *Authenticator) Optional(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := a.authenticateRequest(r)
		if err == nil {
			r = r.WithContext(context.WithValue(r.Context(), principalContextKey, principal))
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Authenticator) RequireAPI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := a.authenticateRequest(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if principal.ActorType == model.ActorTypeRuntime {
			http.Error(w, "runtime credentials cannot access this endpoint", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalContextKey, principal)))
	})
}

func (a *Authenticator) RequireRuntime(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := a.authenticateRequest(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if principal.ActorType != model.ActorTypeRuntime {
			http.Error(w, "runtime credentials required", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalContextKey, principal)))
	})
}

func PrincipalFromContext(ctx context.Context) (model.Principal, bool) {
	principal, ok := ctx.Value(principalContextKey).(model.Principal)
	return principal, ok
}

func (a *Authenticator) authenticateRequest(r *http.Request) (model.Principal, error) {
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if authz == "" {
		return model.Principal{}, errors.New("missing authorization header")
	}

	parts := strings.SplitN(authz, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return model.Principal{}, errors.New("invalid authorization header")
	}
	secret := strings.TrimSpace(parts[1])
	if secret == "" {
		return model.Principal{}, errors.New("empty bearer token")
	}

	if secret == a.BootstrapAdminKey {
		return model.Principal{
			ActorType: model.ActorTypeBootstrap,
			ActorID:   "bootstrap-admin",
			Scopes: map[string]struct{}{
				"platform.admin": {},
			},
		}, nil
	}

	principal, err := a.Store.AuthenticateAPIKey(secret)
	if err == nil {
		return principal, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return model.Principal{}, err
	}

	_, runtimePrincipal, err := a.Store.AuthenticateRuntimeKey(secret)
	if err == nil {
		return runtimePrincipal, nil
	}

	return model.Principal{}, errors.New("invalid credentials")
}
