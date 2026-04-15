package cli

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"fugue/internal/model"
)

func loadDeploymentEnv(workingDir, envFile string, autoDefault bool) (map[string]string, string, error) {
	path, err := resolveEnvFilePath(workingDir, envFile, autoDefault)
	if err != nil {
		return nil, "", err
	}
	if path == "" {
		return nil, "", nil
	}
	env, err := readEnvFile(path)
	if err != nil {
		return nil, "", err
	}
	return env, path, nil
}

func loadTopologyServiceEnvFiles(workingDir string, specs []string) (map[string]map[string]string, []string, error) {
	if len(specs) == 0 {
		return nil, nil, nil
	}

	serviceEnv := map[string]map[string]string{}
	loadedPaths := make([]string, 0, len(specs))
	for _, spec := range specs {
		serviceName, envPath, err := parseServiceEnvFileSpec(spec)
		if err != nil {
			return nil, nil, err
		}
		path, err := resolveEnvFilePath(workingDir, envPath, false)
		if err != nil {
			return nil, nil, err
		}
		env, err := readEnvFile(path)
		if err != nil {
			return nil, nil, err
		}
		if len(env) == 0 {
			loadedPaths = append(loadedPaths, path)
			continue
		}
		serviceVars := serviceEnv[serviceName]
		if serviceVars == nil {
			serviceVars = map[string]string{}
		}
		for key, value := range env {
			serviceVars[key] = value
		}
		serviceEnv[serviceName] = serviceVars
		loadedPaths = append(loadedPaths, path)
	}
	if len(serviceEnv) == 0 {
		return nil, loadedPaths, nil
	}
	return serviceEnv, loadedPaths, nil
}

func loadTopologyServicePersistentStorageOverrides(specs []string) (map[string]model.ServicePersistentStorageOverride, error) {
	if len(specs) == 0 {
		return nil, nil
	}

	overrides := make(map[string]model.ServicePersistentStorageOverride, len(specs))
	for _, spec := range specs {
		serviceName, storageSize, err := parseServiceStorageSizeSpec(spec)
		if err != nil {
			return nil, err
		}
		if _, exists := overrides[serviceName]; exists {
			return nil, fmt.Errorf("duplicate service storage override for %q", serviceName)
		}
		overrides[serviceName] = model.ServicePersistentStorageOverride{StorageSize: storageSize}
	}
	if len(overrides) == 0 {
		return nil, nil
	}
	return overrides, nil
}

func parseServiceEnvFileSpec(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("service env file spec cannot be empty")
	}
	serviceName, envPath, ok := strings.Cut(raw, "=")
	if !ok {
		return "", "", fmt.Errorf("service env file spec must be <service>=<path>")
	}
	serviceName = model.SlugifyOptional(strings.TrimSpace(serviceName))
	if serviceName == "" {
		return "", "", fmt.Errorf("service env file spec requires a service name")
	}
	envPath = strings.TrimSpace(envPath)
	if envPath == "" {
		return "", "", fmt.Errorf("service env file spec requires a file path")
	}
	return serviceName, envPath, nil
}

func parseServiceStorageSizeSpec(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("service storage override cannot be empty")
	}
	serviceName, storageSize, ok := strings.Cut(raw, "=")
	if !ok {
		return "", "", fmt.Errorf("service storage override must be <service>=<size>")
	}
	serviceName = model.SlugifyOptional(strings.TrimSpace(serviceName))
	if serviceName == "" {
		return "", "", fmt.Errorf("service storage override requires a service name")
	}
	storageSize = strings.TrimSpace(storageSize)
	if storageSize == "" {
		return "", "", fmt.Errorf("service storage override requires a storage size")
	}
	return serviceName, storageSize, nil
}

func resolveEnvFilePath(workingDir, envFile string, autoDefault bool) (string, error) {
	envFile = strings.TrimSpace(envFile)
	explicit := envFile != ""
	if envFile == "" {
		if !autoDefault {
			return "", nil
		}
		envFile = ".env"
	}
	if !filepath.IsAbs(envFile) {
		envFile = filepath.Join(strings.TrimSpace(workingDir), envFile)
	}
	info, err := os.Stat(envFile)
	if err != nil {
		if os.IsNotExist(err) && autoDefault && !explicit {
			return "", nil
		}
		if os.IsNotExist(err) {
			return "", fmt.Errorf("env file %s does not exist", envFile)
		}
		return "", fmt.Errorf("stat env file %s: %w", envFile, err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("env file %s is a directory", envFile)
	}
	return envFile, nil
}

func readEnvFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read env file %s: %w", path, err)
	}
	env, err := parseEnvFile(string(data))
	if err != nil {
		return nil, fmt.Errorf("parse env file %s: %w", path, err)
	}
	return env, nil
}

func parseEnvFile(content string) (map[string]string, error) {
	scanner := bufio.NewScanner(strings.NewReader(content))
	env := map[string]string{}
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, rawValue, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("line %d: expected KEY=VALUE", lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("line %d: empty key", lineNo)
		}
		value, err := parseEnvValue(strings.TrimSpace(rawValue))
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		env[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(env) == 0 {
		return nil, nil
	}
	return env, nil
}

func parseEnvValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	switch raw[0] {
	case '"':
		value, rest, err := parseDoubleQuotedEnvValue(raw)
		if err != nil {
			return "", err
		}
		if err := validateEnvValueRemainder(rest); err != nil {
			return "", err
		}
		return value, nil
	case '\'':
		value, rest, err := parseSingleQuotedEnvValue(raw)
		if err != nil {
			return "", err
		}
		if err := validateEnvValueRemainder(rest); err != nil {
			return "", err
		}
		return value, nil
	default:
		return trimEnvInlineComment(raw), nil
	}
}

func parseDoubleQuotedEnvValue(raw string) (string, string, error) {
	escaped := false
	for i := 1; i < len(raw); i++ {
		switch {
		case escaped:
			escaped = false
		case raw[i] == '\\':
			escaped = true
		case raw[i] == '"':
			value, err := strconv.Unquote(raw[:i+1])
			if err != nil {
				return "", "", fmt.Errorf("invalid double-quoted value")
			}
			return value, raw[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("unterminated double-quoted value")
}

func parseSingleQuotedEnvValue(raw string) (string, string, error) {
	for i := 1; i < len(raw); i++ {
		if raw[i] == '\'' {
			return raw[1:i], raw[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("unterminated single-quoted value")
}

func validateEnvValueRemainder(rest string) error {
	rest = strings.TrimSpace(rest)
	if rest == "" || strings.HasPrefix(rest, "#") {
		return nil
	}
	return fmt.Errorf("unexpected trailing content %q", rest)
}

func trimEnvInlineComment(raw string) string {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '#' {
			continue
		}
		if i == 0 || !unicode.IsSpace(rune(raw[i-1])) {
			continue
		}
		return strings.TrimSpace(raw[:i])
	}
	return strings.TrimSpace(raw)
}
