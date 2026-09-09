package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthenticationTimingPreservesSuccessAndRejection(t *testing.T) {
	for _, valid := range []bool{true, false} {
		a := New(nil, "synthetic-bootstrap")
		r := httptest.NewRequest(http.MethodGet, "/v1/example", nil)
		if valid {
			r.Header.Set("Authorization", "Bearer synthetic-bootstrap")
		}
		calls := 0
		r = r.WithContext(WithRequestTimingObserver(r.Context(), func(duration time.Duration) {
			calls++
			if duration < 0 {
				t.Fatal("negative authentication duration")
			}
		}))
		w := httptest.NewRecorder()
		a.RequireAPI(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, ok := PrincipalFromContext(r.Context())
			if !valid || !ok || !principal.IsPlatformAdmin() {
				t.Fatal("timing changed authorization")
			}
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(w, r)
		want := http.StatusUnauthorized
		if valid {
			want = http.StatusNoContent
		}
		if w.Code != want || calls != 1 {
			t.Fatalf("status=%d timing_calls=%d", w.Code, calls)
		}
	}
}
