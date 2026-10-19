package platformconsumer

import (
	"errors"
	"fmt"
	"testing"
)

func TestOfflineProofsRequireOutageRatherThanRejectedAuthority(t *testing.T) {
	for _, err := range []error{ErrUnavailable, fmt.Errorf("transport: %w", ErrUnavailable), &responseStatusError{Code: 503}} {
		if !IsUnavailable(err) {
			t.Fatal("outage not recognized", err)
		}
	}
	for _, err := range []error{nil, ErrNoServingAssignment, ErrDNSBackendNotSelected, ErrAssignmentChanged, errors.New("invalid signature"), &responseStatusError{Code: 401}, &responseStatusError{Code: 403}, &responseStatusError{Code: 404}, &responseStatusError{Code: 409}} {
		if IsUnavailable(err) {
			t.Fatal("rejected authority enabled offline proofs", err)
		}
	}
}
