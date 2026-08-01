package edgeauthkey

import (
	"crypto/subtle"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	KeyFile        = "plan-signing-key"
	KeyIDFile      = "key-id"
	GenerationFile = "key-generation"
)

var generationPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{7,127}$`)

type Snapshot struct {
	Key        string
	KeyID      string
	Generation string
	DataLink   string
}

// Load reads one Kubernetes atomic Secret projection generation. The ..data
// symlink must remain unchanged across all reads; mixed generations fail closed.
func Load(directory string) (Snapshot, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" || !filepath.IsAbs(directory) {
		return Snapshot{}, fmt.Errorf("edge activation key projection directory is invalid")
	}
	before, err := os.Readlink(filepath.Join(directory, "..data"))
	if err != nil || before == "" || filepath.IsAbs(before) || filepath.Base(before) != before || !strings.HasPrefix(before, "..") {
		return Snapshot{}, fmt.Errorf("edge activation key projection generation is unavailable")
	}
	read := func(name string) (string, error) {
		value, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(value)), nil
	}
	key, err := read(KeyFile)
	if err != nil {
		return Snapshot{}, err
	}
	keyID, err := read(KeyIDFile)
	if err != nil {
		return Snapshot{}, err
	}
	generation, err := read(GenerationFile)
	if err != nil {
		return Snapshot{}, err
	}
	after, err := os.Readlink(filepath.Join(directory, "..data"))
	if err != nil || before != after {
		return Snapshot{}, fmt.Errorf("edge activation key projection rotated during read")
	}
	if len(key) < 32 || len(key) > 512 || !generationPattern.MatchString(keyID) || !generationPattern.MatchString(generation) {
		return Snapshot{}, fmt.Errorf("edge activation key projection is invalid")
	}
	return Snapshot{Key: key, KeyID: keyID, Generation: generation, DataLink: before}, nil
}

func Equal(left, right Snapshot) bool {
	return subtle.ConstantTimeCompare([]byte(left.Key), []byte(right.Key)) == 1 && left.KeyID == right.KeyID && left.Generation == right.Generation && left.DataLink == right.DataLink
}
