package inputarchive

import (
	"encoding/base64"
	"testing"
)

func TestPodTokenAudienceCannotBeInferredFromReviewIntersection(t *testing.T) {
	for _, tc := range []struct {
		payload string
		valid   bool
	}{
		{`{"aud":["dedicated"],"other":{"bound":"identity"}}`, true},
		{`{"aud":["dedicated","reader"]}`, false},
		{`{"aud":["reader"],"aud":["dedicated"]}`, false},
		{`{"aud":["dedicated","reader"],"Aud":["dedicated"]}`, false},
		{`{"Aud":["dedicated"]}`, false},
		{`{"aud":"dedicated"}`, false},
		{`{"aud":[]}`, false},
		{`{"aud":["reader"]}`, false},
	} {
		token := "eyJhbGciOiJFUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(tc.payload)) + ".c2ln"
		if PodTokenHasExclusiveAudience(token, "dedicated") != tc.valid {
			t.Fatal(tc.payload)
		}
	}
}
