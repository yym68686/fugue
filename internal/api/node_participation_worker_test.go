package api

import "testing"

func TestParticipationLoopbackOrigin(t *testing.T) {
	for bind, want := range map[string]string{
		":8080": "http://127.0.0.1:8080", "0.0.0.0:8080": "http://127.0.0.1:8080",
		"[::]:8080": "http://[::1]:8080", "127.0.0.1:8080": "http://127.0.0.1:8080",
		"public.example.test:8080": "", "10.0.0.3:8080": "", ":0": "", ":65536": "", "8080": "",
	} {
		if got := participationLoopbackOrigin(bind); got != want {
			t.Errorf("bind %q: want %q, got %q", bind, want, got)
		}
	}
}
