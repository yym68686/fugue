package store

import (
	"context"
	"database/sql"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"fugue/internal/tlsissuance"
	"slices"
	"time"
)

type hostedMemberDirectoryEntry struct {
	Member string `json:"member_id"`
	Kind   string `json:"kind"`
	Set    string `json:"set_id"`
	Child  string `json:"child_id"`
}

const hostedDirectoryInsertSQL = `INSERT INTO fugue_hosted_member_directory(member_id,kind,set_id,child_id,hostname)
 SELECT member_id,kind,set_id,child_id,$1 FROM jsonb_to_recordset($2::jsonb) d(member_id text,kind text,set_id text,child_id text)`

func (s *Store) ListHostedMemberSelections(ctx context.Context, who platformcontrol.PlatformComponentIdentityClaims, a model.PlatformConsumerAssignment, after string) (routeartifact.HostedHintPage, error) {
	if s == nil || s.db == nil || after != "" && !tlsissuance.ValidHostname(after) || who.CredentialID == "" || who.TokenID == "" || who.ExpiresAtUnix <= time.Now().Unix() || who.ScopeKey != "global" ||
		a.ScopeKey != who.ScopeKey || a.ReleaseChannel != "full" || who.ConsumerID() == "" || !slices.Contains(who.ArtifactKinds, a.ArtifactKind) || a.ExpectedConsumerSetID == "" || a.ArtifactID == "" {
		return routeartifact.HostedHintPage{}, ErrInvalidInput
	}
	return pgHostedHintPage(ctx, s.db, who.ConsumerID(), a, after)
}

func pgHostedHintPage(ctx context.Context, db *sql.DB, member string, a model.PlatformConsumerAssignment, after string) (routeartifact.HostedHintPage, error) {
	out := routeartifact.HostedHintPage{Selections: []routeartifact.HostedHint{}}
	rows, err := db.QueryContext(ctx, `SELECT d.hostname,h.fence,h.decision FROM fugue_hosted_member_directory d JOIN fugue_hosted_selections h ON h.hostname=d.hostname
 WHERE d.member_id=$1 AND d.kind=$2 AND d.set_id=$3 AND d.child_id=$4 AND d.hostname>$5 AND h.valid_until>statement_timestamp() ORDER BY d.hostname LIMIT $6`, member, a.ArtifactKind, a.ExpectedConsumerSetID, a.ArtifactID, after, routeartifact.HostedHintLimit+1)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var h routeartifact.HostedHint
		if err = rows.Scan(&h.Host, &h.Fence, &h.Decision); err != nil {
			return routeartifact.HostedHintPage{}, err
		}
		if len(out.Selections) == routeartifact.HostedHintLimit {
			out.Next = out.Selections[len(out.Selections)-1].Host
			break
		}
		out.Selections = append(out.Selections, h)
	}
	return out, rows.Err()
}
