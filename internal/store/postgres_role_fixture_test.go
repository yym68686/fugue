package store

import "crypto/rand"

// An ordinary login must work against both local trust authentication and CI's
// SCRAM authentication. The base32 alphabet is safe in a SQL literal.
func postgresFixturePassword() string {
	return rand.Text()
}
