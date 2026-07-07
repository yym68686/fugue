package runtime

import (
	"fmt"
	"strings"
)

const (
	StrictDrainModeFixedSleep      = "fixed-sleep"
	StrictDrainModeConnectionAware = "connection-aware"

	defaultStrictDrainAgentRepository = "ghcr.io/yym68686/fugue-drain-agent"
	defaultStrictDrainAgentTag        = "latest"
)

type StrictDrainConfig struct {
	Mode                          string
	TimeoutSeconds                int64
	TerminationGraceBufferSeconds int64
	MinReadySeconds               int
	QuietPeriodSeconds            int
	PollIntervalMilliseconds      int
	AgentImageRepository          string
	AgentImageTag                 string
	AgentImageDigest              string
	AgentImagePullPolicy          string
	AgentPort                     int
	NativeSidecarEnabled          bool
}

type RenderOptions struct {
	StrictDrain StrictDrainConfig
	Revision    AppRevisionRenderOptions
}

func DefaultStrictDrainConfig() StrictDrainConfig {
	return StrictDrainConfig{
		Mode:                          StrictDrainModeConnectionAware,
		TimeoutSeconds:                600,
		TerminationGraceBufferSeconds: 30,
		MinReadySeconds:               10,
		QuietPeriodSeconds:            2,
		PollIntervalMilliseconds:      200,
		AgentImageRepository:          defaultStrictDrainAgentRepository,
		AgentImageTag:                 defaultStrictDrainAgentTag,
		AgentImagePullPolicy:          "IfNotPresent",
		AgentPort:                     19090,
		NativeSidecarEnabled:          true,
	}
}

func (cfg StrictDrainConfig) Normalize() StrictDrainConfig {
	defaults := DefaultStrictDrainConfig()
	if strings.TrimSpace(cfg.Mode) == "" {
		cfg.Mode = defaults.Mode
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = defaults.TimeoutSeconds
	}
	if cfg.TerminationGraceBufferSeconds <= 0 {
		cfg.TerminationGraceBufferSeconds = defaults.TerminationGraceBufferSeconds
	}
	if cfg.MinReadySeconds <= 0 {
		cfg.MinReadySeconds = defaults.MinReadySeconds
	}
	if cfg.QuietPeriodSeconds <= 0 {
		cfg.QuietPeriodSeconds = defaults.QuietPeriodSeconds
	}
	if cfg.PollIntervalMilliseconds <= 0 {
		cfg.PollIntervalMilliseconds = defaults.PollIntervalMilliseconds
	}
	if strings.TrimSpace(cfg.AgentImageRepository) == "" {
		cfg.AgentImageRepository = defaults.AgentImageRepository
	}
	if strings.TrimSpace(cfg.AgentImageTag) == "" && strings.TrimSpace(cfg.AgentImageDigest) == "" {
		cfg.AgentImageTag = defaults.AgentImageTag
	}
	if strings.TrimSpace(cfg.AgentImagePullPolicy) == "" {
		cfg.AgentImagePullPolicy = defaults.AgentImagePullPolicy
	}
	if cfg.AgentPort <= 0 {
		cfg.AgentPort = defaults.AgentPort
	}
	return cfg
}

func (cfg StrictDrainConfig) DrainTimeoutSeconds() int64 {
	return cfg.Normalize().TimeoutSeconds
}

func (cfg StrictDrainConfig) TerminationGraceMinSeconds() int64 {
	normalized := cfg.Normalize()
	return normalized.TimeoutSeconds + normalized.TerminationGraceBufferSeconds
}

func (cfg StrictDrainConfig) ConnectionAwareEnabled() bool {
	normalized := cfg.Normalize()
	if !strings.EqualFold(strings.TrimSpace(normalized.Mode), StrictDrainModeConnectionAware) {
		return false
	}
	if !normalized.NativeSidecarEnabled {
		return false
	}
	return strings.TrimSpace(normalized.AgentImageRef()) != ""
}

func (cfg StrictDrainConfig) AgentImageRef() string {
	normalized := cfg
	repo := strings.TrimSpace(normalized.AgentImageRepository)
	if repo == "" {
		return ""
	}
	if digest := strings.TrimSpace(normalized.AgentImageDigest); digest != "" {
		return repo + "@" + digest
	}
	tag := strings.TrimSpace(normalized.AgentImageTag)
	if tag == "" {
		return repo
	}
	return repo + ":" + tag
}

func (cfg StrictDrainConfig) ModeOrFallback() string {
	normalized := cfg.Normalize()
	if normalized.ConnectionAwareEnabled() {
		return StrictDrainModeConnectionAware
	}
	return StrictDrainModeFixedSleep
}

func defaultRenderOptions() RenderOptions {
	return RenderOptions{StrictDrain: DefaultStrictDrainConfig()}
}

func normalizeRenderOptions(options RenderOptions) RenderOptions {
	options.StrictDrain = options.StrictDrain.Normalize()
	options.Revision = NormalizeAppRevisionRenderOptions(options.Revision)
	return options
}

func (cfg StrictDrainConfig) validate() error {
	normalized := cfg.Normalize()
	switch strings.TrimSpace(normalized.Mode) {
	case StrictDrainModeFixedSleep, StrictDrainModeConnectionAware:
	default:
		return fmt.Errorf("unknown strict drain mode %q", normalized.Mode)
	}
	return nil
}
