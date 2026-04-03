package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func readBoolQuery(r *http.Request, key string, defaultValue bool) (bool, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return defaultValue, fmt.Errorf("%s must be true or false", key)
	}

	return value, nil
}

func readOptionalStringQuery(r *http.Request, key string) string {
	return strings.TrimSpace(r.URL.Query().Get(key))
}
