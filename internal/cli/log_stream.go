package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

type sseEvent struct {
	Event string
	ID    string
	Data  []byte
}

type logStreamSourceSpec struct {
	Stream    string `json:"stream"`
	Namespace string `json:"namespace,omitempty"`
	Component string `json:"component,omitempty"`
	JobName   string `json:"job_name,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
	Previous  bool   `json:"previous,omitempty"`
	Phase     string `json:"phase,omitempty"`
}

type buildLogStreamStatusEvent struct {
	Cursor          string   `json:"cursor"`
	OperationID     string   `json:"operation_id"`
	OperationStatus string   `json:"operation_status"`
	JobName         string   `json:"job_name,omitempty"`
	Pods            []string `json:"pods,omitempty"`
	BuildStrategy   string   `json:"build_strategy,omitempty"`
	ErrorMessage    string   `json:"error_message,omitempty"`
	ResultMessage   string   `json:"result_message,omitempty"`
	LastUpdatedAt   string   `json:"last_updated_at,omitempty"`
	CompletedAt     string   `json:"completed_at,omitempty"`
	StartedAt       string   `json:"started_at,omitempty"`
}

type runtimeLogStreamStateEvent struct {
	Cursor    string   `json:"cursor"`
	Component string   `json:"component"`
	Namespace string   `json:"namespace"`
	Selector  string   `json:"selector"`
	Container string   `json:"container"`
	Pods      []string `json:"pods,omitempty"`
	Follow    bool     `json:"follow"`
	Previous  bool     `json:"previous,omitempty"`
}

type logStreamLogEvent struct {
	Cursor    string              `json:"cursor"`
	Source    logStreamSourceSpec `json:"source"`
	Timestamp string              `json:"timestamp,omitempty"`
	Line      string              `json:"line"`
}

type logStreamWarningEvent struct {
	Cursor  string               `json:"cursor"`
	Message string               `json:"message"`
	Source  *logStreamSourceSpec `json:"source,omitempty"`
}

type logStreamEndEvent struct {
	Cursor          string `json:"cursor"`
	Reason          string `json:"reason"`
	OperationStatus string `json:"operation_status,omitempty"`
}

func (c *Client) StreamBuildLogs(appID, operationID string, tailLines int, follow bool, handler func(sseEvent) error) error {
	query := url.Values{}
	query.Set("follow", strconv.FormatBool(follow))
	if strings.TrimSpace(operationID) != "" {
		query.Set("operation_id", strings.TrimSpace(operationID))
	}
	if tailLines > 0 {
		query.Set("tail_lines", strconv.Itoa(tailLines))
	}
	relative := path.Join("/v1/apps", appID, "build-logs", "stream") + "?" + query.Encode()
	return c.streamSSE(relative, handler)
}

func (c *Client) StreamRuntimeLogs(appID string, opts runtimeLogsOptions, follow bool, handler func(sseEvent) error) error {
	query := url.Values{}
	query.Set("follow", strconv.FormatBool(follow))
	if strings.TrimSpace(opts.Component) != "" {
		query.Set("component", strings.TrimSpace(opts.Component))
	}
	if strings.TrimSpace(opts.Pod) != "" {
		query.Set("pod", strings.TrimSpace(opts.Pod))
	}
	if opts.TailLines > 0 {
		query.Set("tail_lines", strconv.Itoa(opts.TailLines))
	}
	if opts.Previous {
		query.Set("previous", "true")
	}
	relative := path.Join("/v1/apps", appID, "runtime-logs", "stream") + "?" + query.Encode()
	return c.streamSSE(relative, handler)
}

func (c *Client) streamSSE(relative string, handler func(sseEvent) error) error {
	httpReq, err := http.NewRequest(http.MethodGet, c.resolveURL(relative), nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := (&http.Client{}).Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		payload, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}
		var apiErr apiError
		if err := json.Unmarshal(payload, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return fmt.Errorf("%s", apiErr.Error)
		}
		if trimmed := strings.TrimSpace(string(payload)); trimmed != "" {
			return fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, trimmed)
		}
		return fmt.Errorf("request failed: status=%d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		eventName string
		eventID   string
		dataLines []string
	)
	dispatch := func() error {
		if eventName == "" && eventID == "" && len(dataLines) == 0 {
			return nil
		}
		event := sseEvent{
			Event: eventName,
			ID:    eventID,
			Data:  []byte(strings.Join(dataLines, "\n")),
		}
		eventName = ""
		eventID = ""
		dataLines = nil
		if handler == nil {
			return nil
		}
		return handler(event)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			field = line
			value = ""
		}
		if strings.HasPrefix(value, " ") {
			value = value[1:]
		}
		switch field {
		case "event":
			eventName = value
		case "id":
			eventID = value
		case "data":
			dataLines = append(dataLines, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := dispatch(); err != nil {
		return err
	}
	return nil
}

func decodeSSEEventData(raw []byte) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}
