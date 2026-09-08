package cli

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type backingServiceResizeRequest struct {
	RuntimeResources model.ResourceSpec `json:"runtime_resources"`
}

type backingServiceResizeResponse struct {
	BackingService model.BackingService `json:"backing_service"`
	Operation      model.Operation      `json:"operation"`
}

type postgresResourceEnvelopeOutput struct {
	Status        string              `json:"status"`
	Resources     *model.ResourceSpec `json:"resources,omitempty"`
	MissingFields []string            `json:"missing_fields,omitempty"`
	Issues        []string            `json:"issues,omitempty"`
}

type backingServicePostgresResourcesOutput struct {
	Bootstrap postgresResourceEnvelopeOutput `json:"bootstrap"`
	Runtime   postgresResourceEnvelopeOutput `json:"runtime_resources"`
	Live      postgresResourceEnvelopeOutput `json:"live"`
}

// backingServiceResizeClientError deliberately excludes the server response
// body. The resize endpoint never needs to echo user-controlled app or
// database state for diagnostics, so status and the retryable bit are
// sufficient and cannot disclose a password from a malformed response.
type backingServiceResizeClientError struct {
	StatusCode int
	Retryable  bool
}

func (e *backingServiceResizeClientError) Error() string {
	if e == nil {
		return "managed Postgres resize request failed"
	}
	parts := []string{"managed Postgres resize request failed"}
	if e.StatusCode > 0 {
		parts = append(parts, fmt.Sprintf("status=%d", e.StatusCode))
	}
	return strings.Join(parts, " ")
}

func validateManualPostgresResizeEnvelope(resources model.ResourceSpec) error {
	if resources.CPUMilliCores <= 0 || resources.MemoryMebibytes <= 0 ||
		resources.CPULimitMilliCores <= 0 || resources.MemoryLimitMebibytes <= 0 {
		return fmt.Errorf("all four resource values must be greater than zero")
	}
	if resources.CPULimitMilliCores < resources.CPUMilliCores {
		return fmt.Errorf("CPU limit must be greater than or equal to CPU request")
	}
	if resources.MemoryLimitMebibytes < resources.MemoryMebibytes {
		return fmt.Errorf("memory limit must be greater than or equal to memory request")
	}
	return nil
}

func backingServicePostgresResourcesForOutput(
	service model.BackingService,
) (backingServicePostgresResourcesOutput, bool) {
	if service.Spec.Postgres == nil {
		return backingServicePostgresResourcesOutput{}, false
	}
	return backingServicePostgresResourcesOutput{
		Bootstrap: classifyPostgresResourceEnvelope(service.Spec.Postgres.Resources),
		Runtime:   classifyPostgresResourceEnvelope(service.Spec.Postgres.RuntimeResources),
		Live: postgresResourceEnvelopeOutput{
			Status: "unknown",
			Issues: []string{"live container resources are not reported by the backing-service API"},
		},
	}, true
}

func classifyPostgresResourceEnvelope(resources *model.ResourceSpec) postgresResourceEnvelopeOutput {
	if resources == nil {
		return postgresResourceEnvelopeOutput{Status: "unknown"}
	}
	copy := *resources
	out := postgresResourceEnvelopeOutput{Status: "complete", Resources: &copy}
	fields := []struct {
		name  string
		value int64
	}{
		{name: "cpu_millicores", value: resources.CPUMilliCores},
		{name: "memory_mebibytes", value: resources.MemoryMebibytes},
		{name: "cpu_limit_millicores", value: resources.CPULimitMilliCores},
		{name: "memory_limit_mebibytes", value: resources.MemoryLimitMebibytes},
	}
	for _, field := range fields {
		if field.value <= 0 {
			out.MissingFields = append(out.MissingFields, field.name)
		}
	}
	if len(out.MissingFields) != 0 {
		out.Status = "partial"
		return out
	}
	if resources.CPULimitMilliCores < resources.CPUMilliCores {
		out.Issues = append(out.Issues, "CPU limit is below request")
	}
	if resources.MemoryLimitMebibytes < resources.MemoryMebibytes {
		out.Issues = append(out.Issues, "memory limit is below request")
	}
	if len(out.Issues) != 0 {
		out.Status = "invalid"
	}
	return out
}

func formatPostgresResourceEnvelopeForOutput(envelope postgresResourceEnvelopeOutput) string {
	formatted := formatResourceSpec(envelope.Resources)
	if formatted == "" {
		return "-"
	}
	return formatted
}

func (c *Client) ResizeBackingService(
	id string,
	runtimeResources model.ResourceSpec,
) (backingServiceResizeResponse, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return backingServiceResizeResponse{}, fmt.Errorf("backing service is required")
	}
	if err := validateManualPostgresResizeEnvelope(runtimeResources); err != nil {
		return backingServiceResizeResponse{}, err
	}
	var response backingServiceResizeResponse
	err := c.doJSON(
		http.MethodPost,
		path.Join("/v1/backing-services", id, "resize"),
		backingServiceResizeRequest{RuntimeResources: runtimeResources},
		&response,
	)
	if err != nil {
		var serverErr *apiServerError
		if errors.As(err, &serverErr) {
			return backingServiceResizeResponse{}, &backingServiceResizeClientError{
				StatusCode: serverErr.StatusCode,
				Retryable:  serverErr.Response.Retryable,
			}
		}
		return backingServiceResizeResponse{}, &backingServiceResizeClientError{}
	}
	if strings.TrimSpace(response.BackingService.ID) != id ||
		strings.TrimSpace(response.Operation.ID) == "" ||
		response.Operation.Type != model.OperationTypeDatabaseResize ||
		strings.TrimSpace(response.Operation.ServiceID) != id {
		return backingServiceResizeResponse{}, fmt.Errorf("managed Postgres resize response has an invalid service or operation identity")
	}
	return response, nil
}

func (c *CLI) newServicePostgresResizeCommand() *cobra.Command {
	opts := struct {
		CPURequest    int64
		MemoryRequest int64
		CPULimit      int64
		MemoryLimit   int64
		Wait          bool
	}{}
	cmd := &cobra.Command{
		Use:   "resize <service>",
		Short: "Queue an explicit in-place resource resize for managed Postgres",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, name := range []string{
				"cpu-millicores", "memory-mebibytes", "cpu-limit-millicores", "memory-limit-mebibytes",
			} {
				if !flagChanged(cmd, name) {
					return fmt.Errorf("all four resource flags are required")
				}
			}
			target := model.ResourceSpec{
				CPUMilliCores:        opts.CPURequest,
				MemoryMebibytes:      opts.MemoryRequest,
				CPULimitMilliCores:   opts.CPULimit,
				MemoryLimitMebibytes: opts.MemoryLimit,
			}
			if err := validateManualPostgresResizeEnvelope(target); err != nil {
				return err
			}

			client, err := c.newClient()
			if err != nil {
				return err
			}
			service, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			service, err = client.GetBackingService(service.ID)
			if err != nil {
				return err
			}
			if !isManagedPostgresBackingService(service) {
				return fmt.Errorf("backing service %q is not an active managed Postgres service", args[0])
			}

			response, err := client.ResizeBackingService(service.ID, target)
			if err != nil {
				return err
			}
			operation := response.Operation
			service = response.BackingService
			if opts.Wait && operationMonitorDone(operation) && operation.Status != model.OperationStatusCompleted {
				return fmt.Errorf("database resize operation %s did not complete successfully; inspect it with fugue operation show %s", operation.ID, operation.ID)
			}
			if opts.Wait && !operationMonitorDone(operation) {
				final, waitErr := c.waitForOperations(client, []model.Operation{operation})
				if waitErr != nil {
					return fmt.Errorf("database resize operation %s did not complete successfully; inspect it with fugue operation show %s", operation.ID, operation.ID)
				}
				if len(final) != 1 || final[0].ID != operation.ID {
					return fmt.Errorf("database resize operation %s returned no terminal result", operation.ID)
				}
				operation = final[0]
				service, err = client.GetBackingService(service.ID)
				if err != nil {
					return fmt.Errorf("database resize operation %s completed but the backing service could not be refreshed", operation.ID)
				}
			}

			if c.wantsJSON() {
				payload := map[string]any{
					"backing_service":             redactBackingServiceForOutput(service),
					"operation":                   redactOperationForOutput(operation),
					"requested_runtime_resources": target,
					"waited":                      opts.Wait,
				}
				if view, ok := backingServicePostgresResourcesForOutput(service); ok {
					payload["postgres_resources"] = view
				}
				if !operationMonitorDone(operation) {
					payload["next_step"] = "fugue operation watch " + operation.ID
				}
				return c.writeJSON(payload)
			}

			pairs := []kvPair{
				{Key: "operation_id", Value: operation.ID},
				{Key: "operation_status", Value: operation.Status},
				{Key: "requested_runtime_resources", Value: formatResourceSpec(&target)},
			}
			if !operationMonitorDone(operation) {
				pairs = append(pairs, kvPair{Key: "next_step", Value: "fugue operation watch " + operation.ID})
			}
			if err := writeKeyValues(c.stdout, pairs...); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return c.renderBackingServiceDetail(client, service)
		},
	}
	cmd.Flags().Int64Var(&opts.CPURequest, "cpu-millicores", 0, "CPU request in millicores")
	cmd.Flags().Int64Var(&opts.MemoryRequest, "memory-mebibytes", 0, "Memory request in MiB")
	cmd.Flags().Int64Var(&opts.CPULimit, "cpu-limit-millicores", 0, "CPU limit in millicores")
	cmd.Flags().Int64Var(&opts.MemoryLimit, "memory-limit-mebibytes", 0, "Memory limit in MiB")
	cmd.Flags().BoolVar(&opts.Wait, "wait", false, "Wait for the database-resize operation to reach a terminal state")
	return cmd
}
