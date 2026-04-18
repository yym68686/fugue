package cli

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (c *CLI) renderProjectDeleteResult(response projectDeleteResponse, waited bool) error {
	finalDeleted := response.Deleted || waited
	pairs := []kvPair{
		{Key: "project", Value: response.Project.Name},
		{Key: "project_id", Value: response.Project.ID},
		{Key: "deleted", Value: fmt.Sprintf("%t", finalDeleted)},
		{Key: "delete_requested", Value: fmt.Sprintf("%t", response.DeleteRequested)},
		{Key: "queued_operations", Value: fmt.Sprintf("%d", response.QueuedOperations)},
		{Key: "already_deleting_apps", Value: fmt.Sprintf("%d", response.AlreadyDeletingApps)},
		{Key: "deleted_backing_services", Value: fmt.Sprintf("%d", response.DeletedBackingServices)},
	}
	if waited {
		pairs = append(pairs, kvPair{Key: "final_state", Value: "deleted"})
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if len(response.Operations) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "[operations]"); err != nil {
			return err
		}
		if err := writeOperationTableWithApps(c.stdout, response.Operations, nil); err != nil {
			return err
		}
	}
	if !waited && response.DeleteRequested {
		_, err := fmt.Fprintf(c.stdout, "next_step=fugue project delete %s --wait\n", response.Project.Name)
		return err
	}
	return nil
}

func (c *CLI) waitForProjectDelete(client *Client, project model.Project, interval time.Duration) (*consoleProjectDetailResponse, *projectStatusResponse, error) {
	if interval <= 0 {
		interval = 3 * time.Second
	}

	var (
		lastDetail       *consoleProjectDetailResponse
		lastStatus       *projectStatusResponse
		lastSnapshotHash [32]byte
		haveSnapshot     bool
	)

	for {
		detail, err := client.TryGetConsoleProjectWithLiveStatus(project.ID, true)
		if err != nil {
			return lastDetail, lastStatus, err
		}
		if detail == nil {
			return lastDetail, lastStatus, nil
		}

		status, err := c.loadProjectStatus(client, *detail)
		if err != nil {
			return lastDetail, lastStatus, err
		}
		lastDetail = detail
		lastStatus = status

		if !c.wantsJSON() {
			snapshotBytes, err := json.Marshal(struct {
				ProjectID   string                 `json:"project_id"`
				Apps        []model.App            `json:"apps"`
				Operations  []model.Operation      `json:"operations"`
				ServiceView *projectStatusResponse `json:"status,omitempty"`
			}{
				ProjectID:   detail.ProjectID,
				Apps:        detail.Apps,
				Operations:  detail.Operations,
				ServiceView: status,
			})
			if err != nil {
				return lastDetail, lastStatus, err
			}
			hashValue := sha256.Sum256(snapshotBytes)
			if !haveSnapshot || hashValue != lastSnapshotHash {
				if haveSnapshot {
					if _, err := fmt.Fprintln(c.stdout); err != nil {
						return lastDetail, lastStatus, err
					}
				}
				if err := c.renderProjectDeleteProgress(*detail, status); err != nil {
					return lastDetail, lastStatus, err
				}
				lastSnapshotHash = hashValue
				haveSnapshot = true
			}
		}

		time.Sleep(interval)
	}
}

func (c *CLI) renderProjectDeleteProgress(detail consoleProjectDetailResponse, status *projectStatusResponse) error {
	pairs := []kvPair{
		{Key: "project", Value: firstNonEmptyTrimmed(detail.ProjectName, detail.ProjectID)},
		{Key: "remaining_apps", Value: fmt.Sprintf("%d", len(detail.Apps))},
		{Key: "active_operations", Value: fmt.Sprintf("%d", countActiveProjectOperations(detail.Operations))},
		{Key: "delete_operations", Value: fmt.Sprintf("%d", countProjectDeleteOperations(detail.Operations))},
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	return renderProjectStatus(c.stdout, status)
}

func countActiveProjectOperations(operations []model.Operation) int {
	active := 0
	for _, op := range operations {
		switch op.Status {
		case model.OperationStatusCompleted, model.OperationStatusFailed:
			continue
		default:
			active++
		}
	}
	return active
}

func countProjectDeleteOperations(operations []model.Operation) int {
	count := 0
	for _, op := range operations {
		if strings.EqualFold(op.Type, model.OperationTypeDelete) {
			count++
		}
	}
	return count
}
