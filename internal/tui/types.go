// Package tui is the human terminal application. It consumes view data and
// capabilities through Provider, without importing the CLI, store or controller.
package tui

import (
	"context"
	"time"
)

type Target struct {
	Kind      string `json:"kind"`
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	TenantID  string `json:"tenant_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

func (t Target) Key() string {
	return t.Kind + ":" + t.TenantID + ":" + t.ProjectID + ":" + t.ID + ":" + t.Name
}

type Field struct {
	Label string `json:"label"`
	Value string `json:"value"`
	Tone  string `json:"tone,omitempty"`
}
type Row struct {
	ID      string          `json:"id"`
	Cells   []string        `json:"cells"`
	Numbers map[int]float64 `json:"numbers,omitempty"`
	Target  *Target         `json:"target,omitempty"`
	Tone    string          `json:"tone,omitempty"`
}
type Table struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}
type Event struct {
	ID       string    `json:"id"`
	At       time.Time `json:"at"`
	Severity string    `json:"severity"`
	Message  string    `json:"message"`
	Target   *Target   `json:"target,omitempty"`
}
type Source struct {
	ID         string    `json:"id"`
	State      string    `json:"state"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
	Message    string    `json:"message,omitempty"`
}
type Point struct {
	At    time.Time `json:"at"`
	Value *float64  `json:"value"`
}
type Series struct {
	ID       string        `json:"id"`
	Label    string        `json:"label"`
	Unit     string        `json:"unit"`
	Source   string        `json:"source"`
	State    string        `json:"state"`
	Interval time.Duration `json:"interval_ns,omitempty"`
	Limit    *float64      `json:"limit,omitempty"`
	Points   []Point       `json:"points"`
}
type Action struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Enabled  bool   `json:"enabled"`
	Reason   string `json:"reason,omitempty"`
	Argument string `json:"argument,omitempty"`
}
type Snapshot struct {
	Target     Target    `json:"target"`
	Title      string    `json:"title"`
	Subtitle   string    `json:"subtitle,omitempty"`
	Status     string    `json:"status"`
	ObservedAt time.Time `json:"observed_at"`
	Fields     []Field   `json:"fields,omitempty"`
	Tables     []Table   `json:"tables,omitempty"`
	Series     []Series  `json:"series,omitempty"`
	Events     []Event   `json:"events,omitempty"`
	Logs       []string  `json:"logs,omitempty"`
	Sources    []Source  `json:"sources,omitempty"`
	Actions    []Action  `json:"actions,omitempty"`
	Admin      bool      `json:"admin"`
}
type Request struct {
	Target  Target
	Section string
	Window  time.Duration
}

func (r Request) Key() string { return r.Target.Key() + ":" + r.Section + ":" + r.Window.String() }

type ActionRequest struct {
	Target   Target
	Action   string
	Argument string
}
type Plan struct {
	ID           string
	Request      ActionRequest
	Title        string
	Effects      []string
	Confirmation string
	ExpiresAt    time.Time
	Precondition string
}
type Receipt struct {
	Operation Target
	Message   string
	Unknown   bool
}
type Notice struct{ Err error }

// Provider implementations own authentication and wire contracts. These methods
// must honor ctx; Execute must never retry a request whose outcome is unknown.
type Provider interface {
	Load(context.Context, Request) (Snapshot, error)
	Plan(context.Context, ActionRequest) (Plan, error)
	Execute(context.Context, Plan) (Receipt, error)
}

// WatchProvider is optional. Providers without an event stream automatically
// use the bounded polling scheduler in Model.
type WatchProvider interface {
	Watch(context.Context, Target, func(Notice)) error
}

// Extension receives only a copied, sanitized view snapshot; it has no client,
// credential, or write capability. Registration is limited to compiled code.
type Extension interface {
	ID() string
	Fields(Snapshot) []Field
}
