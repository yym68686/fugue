package cli

import (
	"fugue/internal/model"
	"net/http"
	"net/url"
	"path"
)

func (c *Client) CreateDatabaseMigration(req map[string]any) (model.DatabaseMigration, error) {
	var resp struct {
		Migration model.DatabaseMigration `json:"migration"`
	}
	err := c.doJSON(http.MethodPost, "/v1/database-migrations", req, &resp)
	return resp.Migration, err
}
func (c *Client) GetDatabaseMigration(id string) (model.DatabaseMigration, error) {
	var resp struct {
		Migration model.DatabaseMigration `json:"migration"`
	}
	err := c.doJSON(http.MethodGet, path.Join("/v1/database-migrations", url.PathEscape(id)), nil, &resp)
	return resp.Migration, err
}
