package dnsserver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"

	"fugue/internal/dnsroutesource"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
)

type dnsSourceCursor struct {
	NodeID      string                               `json:"node_id"`
	AuthorityID string                               `json:"authority_id"`
	Snapshot    model.PlatformDNSRouteSourceSnapshot `json:"snapshot"`
	KeyID       string                               `json:"key_id"`
	Signature   string                               `json:"signature"`
}

func sourceCursorMAC(c dnsSourceCursor, key string) string {
	c.Signature = ""
	raw, _ := json.Marshal(c)
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte("fugue/dns/source-cursor/v1\x00"))
	m.Write(raw)
	return hex.EncodeToString(m.Sum(nil))
}
func (s *Service) advanceDNSRouteCursor(next model.PlatformDNSRouteSourceSnapshot) error {
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return errors.New("DNS source cursor path missing")
	}
	keys := s.platformDNSKeys()
	path := s.Config.CachePath + ".platform-source-cursor.json"
	if raw, err := platformconsumer.ReadFile(path, 20<<20); err == nil {
		var old dnsSourceCursor
		if json.Unmarshal(raw, &old) != nil || old.NodeID != s.Config.DNSNodeID || old.AuthorityID != s.Config.EdgeGroupID {
			return errors.New("DNS source cursor invalid")
		}
		key := ""
		if old.KeyID == keys.PrimaryKeyID {
			key = keys.PrimaryKey
		} else if old.KeyID == keys.PreviousKeyID {
			key = keys.PreviousKey
		}
		if _, revoked := keys.RevokedKeyIDs[strings.ToLower(old.KeyID)]; revoked {
			key = ""
		}
		if key == "" || !hmac.Equal([]byte(old.Signature), []byte(sourceCursorMAC(old, key))) {
			return errors.New("DNS source cursor authentication failed")
		}
		if !dnsroutesource.Monotonic(old.Snapshot, next) {
			return errors.New("DNS routing ledger replay rejected")
		}
		// Keep high water marks for removed approvals. Re-enabling a scope in a
		// later signed config must not resurrect its pre-removal ledger.
		for _, scope := range old.Snapshot.Scopes {
			if !slices.ContainsFunc(next.Scopes, func(n model.PlatformDNSRouteSourceScope) bool { return n.ScopeKey == scope.ScopeKey }) {
				next.Scopes = append(next.Scopes, scope)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if keys.PrimaryKey == "" || keys.PrimaryKeyID == "" {
		return errors.New("DNS source cursor signing key unavailable")
	}
	cursor := dnsSourceCursor{NodeID: s.Config.DNSNodeID, AuthorityID: s.Config.EdgeGroupID, Snapshot: next, KeyID: keys.PrimaryKeyID}
	cursor.Signature = sourceCursorMAC(cursor, keys.PrimaryKey)
	raw, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	return lkgcache.AtomicWriteFile(path, raw, 0600)
}
