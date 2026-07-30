package auth

import (
	"context"
	"net/http"
	"time"

	"fugue/internal/backupidentity"
	"fugue/internal/httpx"
)

const backupObserverIdentityContextKey contextKey = "backup-observer-identity"

// RequireBackupObserver accepts only the dedicated, short-lived observer
// identity and only for GET. It never falls through to tenant API keys,
// workload credentials, node credentials, or platform-component identities.
func (a *Authenticator) RequireBackupObserver(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setBackupObserverNoStoreHeaders(w)
		if r.Method != http.MethodGet {
			httpx.WriteError(w, http.StatusMethodNotAllowed, "backup observer identity is GET-only")
			return
		}
		claims, err := a.authenticateBackupObserverRequest(r, time.Now().UTC())
		if err != nil {
			httpx.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), backupObserverIdentityContextKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func BackupObserverIdentityFromContext(ctx context.Context) (backupidentity.Claims, bool) {
	claims, ok := ctx.Value(backupObserverIdentityContextKey).(backupidentity.Claims)
	return claims, ok
}

func (a *Authenticator) authenticateBackupObserverRequest(r *http.Request, now time.Time) (backupidentity.Claims, error) {
	secret, err := bearerTokenFromRequest(r)
	if err != nil {
		return backupidentity.Claims{}, err
	}
	if a == nil {
		return backupidentity.Claims{}, backupidentity.ErrInvalidIdentity
	}
	return backupidentity.Parse(a.BackupObserverIdentityKeyring, secret, now)
}

func setBackupObserverNoStoreHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "private, no-store, max-age=0")
	w.Header().Set("Pragma", "no-cache")
}
