package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type sseEvent struct {
	Event string
	ID    string
	Data  []byte
}

type streamSSEOptions struct {
	Follow bool
}

type sseHandlerError struct {
	err error
}

func (e sseHandlerError) Error() string {
	return e.err.Error()
}

func (e sseHandlerError) Unwrap() error {
	return e.err
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

type runtimeLogStreamReadyEvent struct {
	Cursor    string `json:"cursor"`
	Stream    string `json:"stream"`
	Follow    bool   `json:"follow"`
	Component string `json:"component"`
	Namespace string `json:"namespace"`
	Selector  string `json:"selector"`
	Container string `json:"container"`
	Previous  bool   `json:"previous,omitempty"`
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
	return c.streamSSEWithOptions(relative, streamSSEOptions{Follow: follow}, handler)
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
	return c.streamSSEWithOptions(relative, streamSSEOptions{Follow: follow}, handler)
}

func (c *Client) streamSSE(relative string, handler func(sseEvent) error) error {
	return c.streamSSEWithOptions(relative, streamSSEOptions{}, handler)
}

func (c *Client) streamSSEWithOptions(relative string, opts streamSSEOptions, handler func(sseEvent) error) error {
	httpClient := &http.Client{}
	retryDelay := 3 * time.Second
	lastEventID := ""
	openedStream := false

	for {
		ended, opened, err := c.streamSSEOnce(httpClient, relative, lastEventID, &retryDelay, func(event sseEvent) error {
			if event.ID != "" {
				lastEventID = event.ID
			} else if cursor := cursorFromSSEData(event.Data); cursor != "" {
				lastEventID = cursor
			}
			if handler == nil {
				return nil
			}
			if err := handler(event); err != nil {
				return sseHandlerError{err: err}
			}
			return nil
		})
		if opened {
			openedStream = true
		}
		if err != nil {
			var handlerErr sseHandlerError
			if errors.As(err, &handlerErr) {
				return handlerErr.err
			}
			if !opts.Follow || !openedStream || !isRetriableSSEStreamError(err) {
				return err
			}
		} else if ended || !opts.Follow {
			return nil
		}
		time.Sleep(retryDelay)
	}
}

func (c *Client) streamSSEOnce(httpClient *http.Client, relative string, lastEventID string, retryDelay *time.Duration, handler func(sseEvent) error) (bool, bool, error) {
	httpReq, err := http.NewRequest(http.MethodGet, c.resolveURL(relative), nil)
	if err != nil {
		return false, false, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	if strings.TrimSpace(lastEventID) != "" {
		httpReq.Header.Set("Last-Event-ID", lastEventID)
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return false, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		payload, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return false, false, fmt.Errorf("read response: %w", readErr)
		}
		var apiErr apiError
		if err := json.Unmarshal(payload, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return false, false, &apiServerError{StatusCode: resp.StatusCode, Response: apiErr}
		}
		if trimmed := strings.TrimSpace(string(payload)); trimmed != "" {
			return false, false, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, trimmed)
		}
		return false, false, fmt.Errorf("request failed: status=%d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		eventName string
		eventID   string
		dataLines []string
		ended     bool
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
		if event.Event == "end" {
			ended = true
		}
		if handler == nil {
			return nil
		}
		if err := handler(event); err != nil {
			return err
		}
		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return false, true, err
			}
			if ended {
				return true, true, nil
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
		case "retry":
			if ms, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && ms >= 0 && retryDelay != nil {
				*retryDelay = time.Duration(ms) * time.Millisecond
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return false, true, err
	}
	if err := dispatch(); err != nil {
		return false, true, err
	}
	if ended {
		return true, true, nil
	}
	return false, true, nil
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

func cursorFromSSEData(raw []byte) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var payload struct {
		Cursor string `json:"cursor"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Cursor)
}

func isRetriableSSEStreamError(err error) bool {
	var apiErr *apiServerError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusTooManyRequests || apiErr.StatusCode >= http.StatusInternalServerError
	}
	return true
}
