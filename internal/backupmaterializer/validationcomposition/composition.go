// Package validationcomposition is the default-off composition root for one
// backup materializer validation cell. It wires three capability-separated
// projections into the pure reconcile, dry-run, validation, and supervision
// layers, but owns no listener, process, Kubernetes SDK, datastore, RBAC,
// workload, chart, release, or production mutation path.
package validationcomposition

import (
	"errors"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	materializerclient "fugue/internal/backupmaterializer/client"
	clientprojected "fugue/internal/backupmaterializer/client/projected"
	"fugue/internal/backupmaterializer/dryrunreconciler"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconciler"
	"fugue/internal/backupmaterializer/secretreader"
	readerprojected "fugue/internal/backupmaterializer/secretreader/projected"
	"fugue/internal/backupmaterializer/secretwriter"
	writerprojected "fugue/internal/backupmaterializer/secretwriter/projected"
	"fugue/internal/backupmaterializer/validationagent"
	"fugue/internal/backupmaterializer/validationcycle"
)

var (
	ErrConfig               = errors.New("backup materializer validation composition configuration invalid")
	ErrInputProjection      = errors.New("backup materializer validation composition input projection unavailable")
	ErrCurrentProjection    = errors.New("backup materializer validation composition current projection unavailable")
	ErrValidationProjection = errors.New("backup materializer validation composition dry-run projection unavailable")
	ErrComposition          = errors.New("backup materializer validation composition unavailable")
	canonicalRunID          = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
)

const (
	minimumDuration       = time.Second
	maximumInterval       = 10 * time.Minute
	maximumAttemptTimeout = time.Minute
)

type Config struct {
	Enabled              bool
	CellKey              string
	RunID                string
	Interval             time.Duration
	AttemptTimeout       time.Duration
	InputProjection      clientprojected.Config
	CurrentProjection    readerprojected.Config
	ValidationProjection writerprojected.Config
	Now                  func() time.Time
}

func (config Config) String() string {
	return "backup materializer validation composition configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

// New returns only the narrow supervisor surface. Enabled construction first
// proves identity, endpoint, projection-root, and timeout separation, then
// validates each local projection and composes the capability graph. It makes
// no network request and starts no timer, goroutine, listener, or process.
// Disabled construction performs no filesystem access and retains no input.
func New(config Config, logger *log.Logger) (*validationagent.Service, error) {
	if !config.Enabled {
		service, err := validationagent.New(validationagent.Config{Enabled: false}, logger)
		if err != nil {
			return nil, ErrComposition
		}
		return service, nil
	}
	if !validBoundary(config) || !projectionRootsDistinct(config) {
		return nil, ErrConfig
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}

	inputConfig := config.InputProjection
	inputConfig.Now = now
	inputClient, err := clientprojected.New(inputConfig)
	if err != nil || inputClient == nil || !inputClient.Enabled() {
		return nil, ErrInputProjection
	}
	currentReader, err := readerprojected.New(config.CurrentProjection)
	if err != nil || currentReader == nil || !currentReader.Enabled() {
		return nil, ErrCurrentProjection
	}
	validationConfig := config.ValidationProjection
	validationConfig.Now = now
	dryRunWriter, err := writerprojected.New(validationConfig)
	if err != nil || dryRunWriter == nil || !dryRunWriter.Enabled() {
		return nil, ErrValidationProjection
	}

	source, err := reconciler.New(reconciler.Config{
		Enabled: true, CellKey: config.CellKey, DesiredSource: inputClient, CurrentSource: currentReader, Now: now,
	})
	if err != nil || source == nil || !source.Enabled() {
		return nil, ErrComposition
	}
	validator, err := dryrunreconciler.New(dryrunreconciler.Config{
		Enabled: true, CellKey: config.CellKey, Validator: dryRunWriter, Now: now,
	})
	if err != nil || validator == nil || !validator.Enabled() {
		return nil, ErrComposition
	}
	cycle, err := validationcycle.New(validationcycle.Config{
		Enabled: true, CellKey: config.CellKey, Source: source, Validator: validator,
	})
	if err != nil || cycle == nil || !cycle.Enabled() {
		return nil, ErrComposition
	}
	service, err := validationagent.New(validationagent.Config{
		Enabled: true, CellKey: config.CellKey, Cycle: cycle,
		Interval: config.Interval, AttemptTimeout: config.AttemptTimeout, Now: now,
	}, logger)
	if err != nil || service == nil || !service.Enabled() {
		return nil, ErrComposition
	}
	return service, nil
}

func validBoundary(config Config) bool {
	inputAuthority, inputURLValid := canonicalHTTPSAuthority(config.InputProjection.BaseURL)
	currentAuthority, currentURLValid := canonicalHTTPSAuthority(config.CurrentProjection.APIServerURL)
	validationAuthority, validationURLValid := canonicalHTTPSAuthority(config.ValidationProjection.APIServerURL)
	inputRoot, inputRootValid := canonicalProjectionRoot(config.InputProjection.ProjectionRoot)
	currentRoot, currentRootValid := canonicalProjectionRoot(config.CurrentProjection.ProjectionRoot)
	validationRoot, validationRootValid := canonicalProjectionRoot(config.ValidationProjection.ProjectionRoot)
	if _, err := materialization.SecretIdentityForCell(config.CellKey); err != nil || !canonicalRunID.MatchString(config.RunID) ||
		!config.InputProjection.Enabled || !config.CurrentProjection.Enabled || !config.ValidationProjection.Enabled ||
		config.InputProjection.ExpectedCellKey != config.CellKey ||
		config.CurrentProjection.ExpectedCellKey != config.CellKey ||
		config.ValidationProjection.ExpectedCellKey != config.CellKey ||
		config.InputProjection.ExpectedRunID != config.RunID ||
		!inputURLValid || !currentURLValid || !validationURLValid || inputAuthority == currentAuthority ||
		currentAuthority != validationAuthority || !inputRootValid || !currentRootValid || !validationRootValid ||
		inputRoot == currentRoot || inputRoot == validationRoot || currentRoot == validationRoot ||
		!boundedDuration(config.Interval, minimumDuration, maximumInterval) ||
		!boundedDuration(config.AttemptTimeout, minimumDuration, maximumAttemptTimeout) {
		return false
	}
	inputTimeout := config.InputProjection.RequestTimeout
	if inputTimeout == 0 {
		inputTimeout = materializerclient.DefaultRequestTimeout
	}
	currentTimeout := config.CurrentProjection.RequestTimeout
	if currentTimeout == 0 {
		currentTimeout = secretreader.DefaultRequestTimeout
	}
	validationTimeout := config.ValidationProjection.RequestTimeout
	if validationTimeout == 0 {
		validationTimeout = secretwriter.DefaultRequestTimeout
	}
	return inputTimeout <= config.AttemptTimeout && currentTimeout <= config.AttemptTimeout &&
		validationTimeout <= config.AttemptTimeout
}

func canonicalHTTPSAuthority(raw string) (string, bool) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.String() != raw ||
		strings.HasSuffix(parsed.Host, ":") {
		return "", false
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	} else {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return "", false
		}
	}
	hostname := strings.ToLower(parsed.Hostname())
	if strings.HasSuffix(hostname, ".") {
		hostname = strings.TrimSuffix(hostname, ".")
	}
	if hostname == "" {
		return "", false
	}
	return net.JoinHostPort(hostname, port), true
}

func canonicalProjectionRoot(root string) (string, bool) {
	return root, root != "" && strings.TrimSpace(root) == root && filepath.IsAbs(root) &&
		filepath.Clean(root) == root && root != string(filepath.Separator)
}

func projectionRootsDistinct(config Config) bool {
	roots := []string{
		config.InputProjection.ProjectionRoot,
		config.CurrentProjection.ProjectionRoot,
		config.ValidationProjection.ProjectionRoot,
	}
	infos := make([]os.FileInfo, len(roots))
	for index, root := range roots {
		info, err := os.Stat(root)
		if err != nil {
			// The owning projected adapter returns the fixed stage-specific error.
			return true
		}
		infos[index] = info
	}
	return !os.SameFile(infos[0], infos[1]) && !os.SameFile(infos[0], infos[2]) &&
		!os.SameFile(infos[1], infos[2])
}

func boundedDuration(value, minimum, maximum time.Duration) bool {
	return value >= minimum && value <= maximum && value%time.Millisecond == 0
}
