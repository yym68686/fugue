package cli

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
)

func (c *CLI) waitForImportBundle(client *Client, bundle importBundle) (importBundle, *appOverviewDiagnosis, error) {
	order := make([]string, 0, len(bundle.Operations))
	tracked := make(map[string]model.Operation, len(bundle.Operations))
	for _, op := range bundle.Operations {
		if id := strings.TrimSpace(op.ID); id != "" {
			order = append(order, id)
			tracked[id] = op
		}
	}
	if len(tracked) == 0 {
		diagnosis, err := c.buildImportBundleDiagnosis(client, bundle.PrimaryApp)
		if err != nil {
			return bundle, nil, err
		}
		return bundle, diagnosis, nil
	}

	filters := deployBundleStatusFilters(bundle)
	lastStatus := make(map[string]string, len(tracked))
	var lastSnapshotHash [32]byte
	haveSnapshot := false
	transientErrors := deployWaitTransientErrorTracker{}

	for {
		currentOps := make([]model.Operation, 0, len(order))
		pending := 0
		pollInterrupted := false
		for _, id := range order {
			base, ok := tracked[id]
			if !ok {
				continue
			}
			current, err := client.GetOperation(id)
			if err != nil {
				retry, retryErr := transientErrors.shouldRetry(c, err)
				if retryErr != nil {
					return bundle, nil, retryErr
				}
				if retry {
					pollInterrupted = true
					break
				}
			}
			tracked[id] = current
			currentOps = append(currentOps, current)

			status := strings.TrimSpace(current.Status)
			if status != lastStatus[id] {
				if len(order) == 1 {
					c.progressf("operation_status=%s", status)
				} else {
					c.progressf("operation_id=%s operation_status=%s", current.ID, status)
				}
				lastStatus[id] = status
			}

			if strings.EqualFold(strings.TrimSpace(current.Type), model.OperationTypeImport) &&
				strings.EqualFold(status, model.OperationStatusCompleted) {
				if linkedID := queuedDeployOperationID(current.ResultMessage); linkedID != "" {
					if _, exists := tracked[linkedID]; !exists {
						order = append(order, linkedID)
						tracked[linkedID] = model.Operation{
							ID:    linkedID,
							AppID: firstNonEmptyTrimmed(strings.TrimSpace(current.AppID), strings.TrimSpace(base.AppID)),
							Type:  model.OperationTypeDeploy,
						}
					}
				}
			}

			switch status {
			case model.OperationStatusCompleted:
			case model.OperationStatusFailed:
				return bundle, nil, c.operationFailure(client, current)
			default:
				pending++
			}
		}
		if pollInterrupted {
			sleepDeployWaitPoll()
			continue
		}

		currentApps, err := fetchFinalApps(client, bundle.Apps, currentOps)
		if err != nil {
			retry, retryErr := transientErrors.shouldRetry(c, err)
			if retryErr != nil {
				return bundle, nil, retryErr
			}
			if retry {
				sleepDeployWaitPoll()
				continue
			}
		}
		transientErrors.reset()
		if err := c.renderDeployProgressSnapshot(client, currentApps, currentOps, filters, &lastSnapshotHash, &haveSnapshot); err != nil {
			return bundle, nil, err
		}

		if pending == 0 {
			bundle.Operations = currentOps
			bundle.Apps = currentApps
			if op, ok := findOperationByID(currentOps, bundle.PrimaryOp.ID); ok {
				bundle.PrimaryOp = op
			}
			if app, ok := findAppByID(currentApps, bundle.PrimaryApp.ID); ok {
				bundle.PrimaryApp = app
			} else if len(currentApps) > 0 {
				bundle.PrimaryApp = currentApps[0]
			}
			diagnosis, err := c.buildImportBundleDiagnosis(client, bundle.PrimaryApp)
			if err != nil {
				if isTransientDeployWaitError(err) {
					c.progressf("warning=deploy diagnosis unavailable after completion: %v", err)
					diagnosis = nil
				} else {
					return bundle, nil, err
				}
			}
			return bundle, diagnosis, nil
		}

		sleepDeployWaitPoll()
	}
}

func (c *CLI) renderDeployProgressSnapshot(client *Client, apps []model.App, operations []model.Operation, filters projectStatusFilters, lastHash *[32]byte, haveSnapshot *bool) error {
	if c.wantsJSON() {
		return nil
	}
	status, err := c.buildProjectStatusFromAppsAndOperations(client, apps, operations, filters)
	if err != nil {
		c.progressf("warning=deploy service status unavailable: %v", err)
		return nil
	}
	snapshotBytes, err := json.Marshal(struct {
		Operations []model.Operation      `json:"operations"`
		Status     *projectStatusResponse `json:"status,omitempty"`
	}{
		Operations: operations,
		Status:     status,
	})
	if err != nil {
		return err
	}
	hashValue := sha256.Sum256(snapshotBytes)
	if *haveSnapshot && hashValue == *lastHash {
		return nil
	}
	if *haveSnapshot {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
	}
	if err := renderProjectStatus(c.stdout, status); err != nil {
		return err
	}
	*lastHash = hashValue
	*haveSnapshot = true
	return nil
}

func deployBundleStatusFilters(bundle importBundle) projectStatusFilters {
	if bundle.Plan != nil {
		return topologyPlanStatusFilters(bundle.Plan, bundle.Apps)
	}
	filters := projectStatusFilters{
		AppIDs:       map[string]struct{}{},
		DeleteAppIDs: map[string]struct{}{},
		Services:     map[string]struct{}{},
	}
	for _, app := range bundle.Apps {
		if value := strings.TrimSpace(app.ID); value != "" {
			filters.AppIDs[value] = struct{}{}
		}
		if service := strings.TrimSpace(projectServiceLabel(app)); service != "" {
			filters.Services[service] = struct{}{}
		}
	}
	for _, op := range bundle.Operations {
		if strings.EqualFold(strings.TrimSpace(op.Type), model.OperationTypeDelete) {
			if value := strings.TrimSpace(op.AppID); value != "" {
				filters.DeleteAppIDs[value] = struct{}{}
			}
		}
	}
	return filters
}
