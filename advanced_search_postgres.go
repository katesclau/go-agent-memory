package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"github.com/pgvector/pgvector-go"
)

func (sm *SupabaseMemory) SearchMessages(
	ctx context.Context,
	req SearchMessagesRequest,
) ([]SearchResult, error) {
	if err := validateSearchRequest(req); err != nil {
		return nil, err
	}
	if req.Mode == SearchModeKeyword {
		return sm.searchKeywordMessages(ctx, req)
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
		where = appendSearchClause(where, "NOT ("+postgresHistoricalPredicate(req.Filter)+")")
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
			CASE WHEN %s THEN 1 ELSE 0 END,
			%s`, postgresHistoricalPredicate(req.Filter), order)
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

func (sm *SupabaseMemory) searchKeywordMessages(
	ctx context.Context,
	req SearchMessagesRequest,
) ([]SearchResult, error) {
	words := keywordWords(req.Query)
	if len(words) == 0 {
		return []SearchResult{}, nil
	}
	limit := req.Limit
	if limit == 0 {
		limit = sm.config.DefaultSearchLimit
		if limit == 0 {
			limit = 5
		}
	}
	where, args, err := postgresMessageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	if req.TemporalPolicy == TemporalPolicyCurrentOnly {
		where = appendSearchClause(where, "NOT ("+postgresHistoricalPredicate(req.Filter)+")")
	}
	args = append(args, words)
	wordsArg := len(args)
	overlap := fmt.Sprintf(`(
		SELECT count(*)
		FROM unnest($%d::text[]) AS query_word(word)
		WHERE lower(content) LIKE '%%' || query_word.word || '%%'
	)`, wordsArg)
	where = appendSearchClause(where, overlap+" > 0")
	args = append(args, limit)
	limitArg := len(args)

	order := overlap + " DESC, created_at DESC, message_id ASC"
	if req.TemporalPolicy == TemporalPolicyCurrentFirst {
		order = fmt.Sprintf(
			"CASE WHEN %s THEN 1 ELSE 0 END, %s",
			postgresHistoricalPredicate(req.Filter), order,
		)
	}
	query := fmt.Sprintf(`
		SELECT message_id, role, content, metadata, created_at,
		       (%s)::real / cardinality($%d::text[]) AS score,
		       1 - ((%s)::real / cardinality($%d::text[])) AS distance
		FROM agent_messages
		%s
		ORDER BY %s
		LIMIT $%d`,
		overlap, wordsArg, overlap, wordsArg, where, order, limitArg,
	)
	rows, err := sm.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("keyword search messages: %w", err)
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
			return nil, fmt.Errorf("scan keyword search result: %w", err)
		}
		if err := decodeMetadata(metadataJSON, &result.Message.Metadata); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func keywordWords(query string) []string {
	raw := strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	seen := make(map[string]struct{}, len(raw))
	words := make([]string, 0, len(raw))
	for _, word := range raw {
		if word == "" {
			continue
		}
		if _, exists := seen[word]; exists {
			continue
		}
		seen[word] = struct{}{}
		words = append(words, word)
	}
	return words
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
