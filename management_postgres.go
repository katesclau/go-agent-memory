package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

func (sm *SupabaseMemory) ListMessages(
	ctx context.Context,
	req ListMessagesRequest,
) ([]Message, error) {
	if err := validateListRequest(req); err != nil {
		return nil, err
	}
	where, args, err := postgresMessageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	direction := "DESC"
	if req.Order == MessageOrderOldest {
		direction = "ASC"
	}
	limit := req.Limit
	if limit == 0 {
		limit = 100
	}
	args = append(args, limit, req.Offset)
	query := fmt.Sprintf(`
		SELECT message_id, role, content, metadata, created_at
		FROM agent_messages
		%s
		ORDER BY created_at %s, message_id ASC
		LIMIT $%d OFFSET $%d`,
		where, direction, len(args)-1, len(args),
	)
	rows, err := sm.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()
	var messages []Message
	for rows.Next() {
		msg, scanErr := scanManagedMessage(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		messages = append(messages, msg)
	}
	return messages, rows.Err()
}

func (sm *SupabaseMemory) CountMessages(
	ctx context.Context,
	filter MessageFilter,
) (int64, error) {
	where, args, err := postgresMessageFilter(filter)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := sm.db.QueryRow(ctx,
		"SELECT count(*) FROM agent_messages "+where,
		args...,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count messages: %w", err)
	}
	return count, nil
}

func (sm *SupabaseMemory) DeleteMessages(
	ctx context.Context,
	req DeleteMessagesRequest,
) (int64, error) {
	count, _, err := sm.deleteMessages(ctx, req)
	return count, err
}

func (sm *SupabaseMemory) deleteMessages(
	ctx context.Context,
	req DeleteMessagesRequest,
) (int64, []string, error) {
	if err := validateMessageFilter(req.Filter); err != nil {
		return 0, nil, err
	}
	if emptyMessageFilter(req.Filter) && !req.AllowAll {
		return 0, nil, ErrDeleteFilterRequired
	}
	where, args, err := postgresMessageFilter(req.Filter)
	if err != nil {
		return 0, nil, err
	}
	rows, err := sm.db.Query(ctx,
		"DELETE FROM agent_messages "+where+" RETURNING session_id",
		args...,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("delete messages: %w", err)
	}
	defer rows.Close()
	sessions := make(map[string]struct{})
	var count int64
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return 0, nil, fmt.Errorf("scan deleted message session: %w", err)
		}
		count++
		sessions[sessionID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	sessionIDs := make([]string, 0, len(sessions))
	for sessionID := range sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}
	return count, sessionIDs, nil
}

func postgresMessageFilter(filter MessageFilter) (string, []interface{}, error) {
	if err := validateMessageFilter(filter); err != nil {
		return "", nil, err
	}
	var clauses []string
	var args []interface{}
	add := func(clause string, value interface{}) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if len(filter.MessageIDs) > 0 {
		add("message_id = ANY($%d::text[])", filter.MessageIDs)
	}
	if value := strings.TrimSpace(filter.SessionID); value != "" {
		add("session_id = $%d", value)
	}
	if value := strings.TrimSpace(filter.UserID); value != "" {
		add("user_id = $%d", value)
	}
	if len(filter.ExtraEquals) > 0 {
		encoded, err := json.Marshal(filter.ExtraEquals)
		if err != nil {
			return "", nil, fmt.Errorf("encode extra metadata filter: %w", err)
		}
		add("coalesce(metadata->'extra', '{}'::jsonb) @> $%d::jsonb", encoded)
	}
	for key, value := range filter.ExtraEqualFold {
		args = append(args, key, value)
		clauses = append(clauses, fmt.Sprintf(
			"lower(coalesce(metadata->'extra'->>$%d, '')) = lower($%d)",
			len(args)-1, len(args),
		))
	}
	if filter.CreatedAfter != nil {
		add("created_at > $%d", filter.CreatedAfter.UTC())
	}
	if filter.CreatedBefore != nil {
		add("created_at < $%d", filter.CreatedBefore.UTC())
	}
	switch filter.TemporalState {
	case TemporalStateCurrent:
		clauses = append(clauses, "NOT ("+postgresHistoricalVersionPredicate()+")")
	case TemporalStateHistorical:
		clauses = append(clauses, postgresHistoricalVersionPredicate())
	}
	if len(clauses) == 0 {
		return "", args, nil
	}
	return "WHERE " + strings.Join(clauses, " AND "), args, nil
}

func postgresHistoricalVersionPredicate() string {
	const version = "metadata->'extra'->'_memory_version'"
	return fmt.Sprintf(`(
		jsonb_typeof(%[1]s) = 'object'
		AND nullif(%[1]s->>'namespace', '') IS NOT NULL
		AND nullif(%[1]s->>'key', '') IS NOT NULL
		AND nullif(%[1]s->>'revision', '') IS NOT NULL
		AND (%[1]s->>'version') ~ '^[1-9][0-9]*$'
		AND agent_memory_try_bigint(%[1]s->>'version') IS NOT NULL
		AND (
			lower(coalesce(%[1]s->>'status', 'active')) = 'superseded'
			OR agent_memory_try_timestamptz(%[1]s->>'valid_until') <= CURRENT_TIMESTAMP
		)
	)`, version)
}
