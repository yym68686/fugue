// Package queryrow reads fields decoded from telemetry query results.
package queryrow

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func String(row map[string]any, key string) string {
	value, ok := row[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func Float(row map[string]any, key string) float64 {
	value, _ := OptionalFloat(row, key)
	return value
}

func FiniteFloat(row map[string]any, key string) float64 {
	value := Float(row, key)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func OptionalFloat(row map[string]any, key string) (float64, bool) {
	value, ok := row[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
