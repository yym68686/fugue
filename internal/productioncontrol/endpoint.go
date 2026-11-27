package productioncontrol

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
)

// LoadJournalEndpoint reads a separately mounted runtime configuration. It
// deliberately does not echo connection strings or filesystem error details.
func LoadJournalEndpoint(path string) (*pgx.ConnConfig, Approvers, string, error) {
	if !filepath.IsAbs(path) {
		return nil, nil, "", errors.New("production journal requires an absolute path to its configuration")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 32<<10 {
		return nil, nil, "", errors.New("production journal configuration file is unavailable or invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, "", errors.New("could not open the production journal configuration")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (32<<10)+1))
	var settings struct {
		Cluster  string            `json:"cluster"`
		Database string            `json:"database"`
		Owners   map[string][]byte `json:"ownerPublicKeys"`
	}
	if err != nil || len(raw) > 32<<10 || decodeControlJSON(raw, &settings) != nil ||
		settings.Database == "" || !tokenPattern.MatchString(settings.Cluster) || len(settings.Owners) == 0 {
		return nil, nil, "", errors.New("production journal settings require an independent database and owner public keys")
	}
	cfg, err := pgx.ParseConfig(settings.Database)
	if err != nil || cfg.Host == "" || cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify ||
		cfg.TLSConfig.ServerName != cfg.Host || len(cfg.Fallbacks) != 0 {
		return nil, nil, "", errors.New("production journal requires one hostname-verified PostgreSQL TLS endpoint")
	}
	cfg.ConnectTimeout = 5 * time.Second
	trusted := make(Approvers, len(settings.Owners))
	for id, key := range settings.Owners {
		trusted[id] = TrustedApprover{ClusterID: settings.Cluster, PublicKey: key}
	}
	return cfg, trusted, settings.Cluster, nil
}
