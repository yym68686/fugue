package model

type SearchResult struct {
	Kind          string            `json:"kind"`
	ID            string            `json:"id"`
	Name          string            `json:"name,omitempty"`
	TenantID      string            `json:"tenant_id,omitempty"`
	TenantName    string            `json:"tenant_name,omitempty"`
	ProjectID     string            `json:"project_id,omitempty"`
	ProjectName   string            `json:"project_name,omitempty"`
	AppID         string            `json:"app_id,omitempty"`
	AppName       string            `json:"app_name,omitempty"`
	PublicURL     string            `json:"public_url,omitempty"`
	InternalURL   string            `json:"internal_url,omitempty"`
	Status        string            `json:"status,omitempty"`
	Type          string            `json:"type,omitempty"`
	RuntimeID     string            `json:"runtime_id,omitempty"`
	RuntimeName   string            `json:"runtime_name,omitempty"`
	Ref           string            `json:"ref,omitempty"`
	Summary       string            `json:"summary,omitempty"`
	Score         int               `json:"score"`
	MatchedFields []string          `json:"matched_fields,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

type SearchResponse struct {
	Query   string         `json:"query"`
	Types   []string       `json:"types,omitempty"`
	Results []SearchResult `json:"results"`
	Limit   int            `json:"limit"`
}
