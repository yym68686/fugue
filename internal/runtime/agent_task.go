package runtime

import "fugue/internal/model"

type AgentTask struct {
	Operation model.Operation `json:"operation"`
	App       model.App       `json:"app"`
}
