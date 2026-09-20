package livediagnostics

import (
	"errors"
	"sync"
)

// CatalogCache retains only signature-verified configuration. A revoked signing
// key cannot authorize a cached catalog even when the replacement is invalid.
type CatalogCache struct {
	mu     sync.Mutex
	signed []byte
}

func (c *CatalogCache) Load(data []byte, keys map[string]string) (VerifiedCatalog, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	verified, err := VerifyCatalog(data, keys)
	if err == nil {
		c.signed = append(c.signed[:0], data...)
		return verified, nil
	}
	if len(c.signed) > 0 {
		old, oldErr := VerifyCatalog(c.signed, keys)
		if oldErr == nil {
			old.Status = "last_known_good"
			return old, nil
		}
	}
	return VerifiedCatalog{}, errors.New("no trusted diagnostic catalog is available: " + err.Error())
}

// LoadPublishedCatalog also supports a cold process after a rejected update.
// The previous envelope is verified against the current trust policy, so key
// revocation remains effective across both persisted and in-memory fallbacks.
func LoadPublishedCatalog(current, previous []byte, keys map[string]string, cache *CatalogCache) (VerifiedCatalog, error) {
	verified, err := VerifyCatalog(current, keys)
	if err == nil {
		if cache != nil {
			return cache.Load(current, keys)
		}
		return verified, nil
	}
	if len(previous) > 0 {
		prior, priorErr := VerifyCatalog(previous, keys)
		if priorErr == nil {
			if cache != nil {
				prior, priorErr = cache.Load(previous, keys)
				if priorErr != nil {
					return VerifiedCatalog{}, priorErr
				}
			}
			prior.Status = "last_known_good"
			return prior, nil
		}
	}
	if cache != nil {
		return cache.Load(current, keys)
	}
	return VerifiedCatalog{}, err
}
