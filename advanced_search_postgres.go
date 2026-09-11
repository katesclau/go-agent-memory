package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pgvector/pgvector-go"
)

func (sm *SupabaseMemory) SearchMessages(
	ctx context.Context,
	req SearchMessagesRequest,
) ([]SearchResult, error) {
	if err := validateSearchRequest(req); err != nil {
		return nil, err
	}
	embedding := req.Embedding
	if len(embedding) == 0 {
		var err error
		embedding, err = sm.generateEmbedding(ctx, strings.TrimSpace(req.Query))
		if err != nil {
			return nil, fmt.Errorf("generate search embedding: %w", err)
		}
	}
	limit := req.Limit
	if limit == 0 {
		limit = sm.config.DefaultSearchLimit
		if limit == 0 {
			limit = 5
		}
	}
	threshold := req.Threshold
	if threshold <= 0 {
		threshold = sm.config.DefaultSearchThreshold
		if threshold <= 0 {
			threshold = 0.7
		}
	}

	where, args, err := postgresMessageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	where = appendSearchClause(where, "embedding IS NOT NULL")
	if req.TemporalPolicy == TemporalPolicyCurrentOnly {
		where = appendSearchClause(where,
			"lower(coalesce(metadata->'extra'->'_memory_version'->>'status', 'active')) <> 'superseded'")
	}
	args = append(args, pgvector.NewVector(embedding), threshold, limit)
	vectorArg := len(args) - 2
	thresholdArg := len(args) - 1
	limitArg := len(args)
	where = appendSearchClause(where,
		fmt.Sprintf("1 - (embedding <=> $%d::vector) > $%d", vectorArg, thresholdArg))

	order := fmt.Sprintf("embedding <=> $%d::vector, created_at DESC, message_id ASC", vectorArg)
	if req.TemporalPolicy == TemporalPolicyCurrentFirst {
		order = fmt.Sprintf(`
			CASE WHEN lower(coalesce(
				metadata->'extra'->'_memory_version'->>'status', 'active'
			)) = 'superseded' THEN 1 ELSE 0 END,
			%s`, order)
	}
	query := fmt.Sprintf(`
		SELECT message_id, role, content, metadata, created_at,
		       1 - (embedding <=> $%d::vector) AS score,
		       embedding <=> $%d::vector AS distance
		FROM agent_messages
		%s
		ORDER BY %s
		LIMIT $%d`,
		vectorArg, vectorArg, where, order, limitArg,
	)
	rows, err := sm.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search messages: %w", err)
	}
	defer rows.Close()
	var results []SearchResult
	for rows.Next() {
		var result SearchResult
		var metadataJSON []byte
		if err := rows.Scan(
			&result.Message.ID,
			&result.Message.Role,
			&result.Message.Content,
			&metadataJSON,
			&result.Message.Timestamp,
			&result.Score,
			&result.Distance,
		); err != nil {
			return nil, fmt.Errorf("scan search result: %w", err)
		}
		if err := decodeMetadata(metadataJSON, &result.Message.Metadata); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func appendSearchClause(where, clause string) string {
	if where == "" {
		return "WHERE " + clause
	}
	return where + " AND " + clause
}

func decodeMetadata(data []byte, metadata *Metadata) error {
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, metadata); err != nil {
		return fmt.Errorf("decode message metadata: %w", err)
	}
	return nil
}
