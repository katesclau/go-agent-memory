package memory

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"
)

var (
	ErrSearchInputRequired   = errors.New("query or embedding is required")
	ErrInvalidTemporalPolicy = errors.New("invalid temporal policy")
	ErrInvalidSearchLimit    = errors.New("search limit must not be negative")
)

func validateSearchRequest(req SearchMessagesRequest) error {
	if err := validateMessageFilter(req.Filter); err != nil {
		return err
	}
	switch req.Mode {
	case SearchModeSemantic, SearchModeKeyword:
	default:
		return errors.New("invalid search mode")
	}
	if strings.TrimSpace(req.Query) == "" && (len(req.Embedding) == 0 || req.Mode == SearchModeKeyword) {
		return ErrSearchInputRequired
	}
	if req.Limit < 0 {
		return ErrInvalidSearchLimit
	}
	switch req.TemporalPolicy {
	case TemporalPolicyAllVersions, TemporalPolicyCurrentOnly, TemporalPolicyCurrentFirst:
		return nil
	default:
		return ErrInvalidTemporalPolicy
	}
}

func (sm *SessionOnlyMemory) SearchMessages(
	ctx context.Context,
	req SearchMessagesRequest,
) ([]SearchResult, error) {
	if err := validateSearchRequest(req); err != nil {
		return nil, err
	}
	if req.Mode != SearchModeKeyword && len(req.Embedding) > 0 {
		return nil, errors.New("embedding search is not supported in session-only mode")
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
	now := time.Now()
	query := strings.ToLower(strings.TrimSpace(req.Query))

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	var results []SearchResult
	for _, messages := range sm.sessions {
		for _, msg := range messages {
			if !messageMatchesFilter(msg, req.Filter, now) {
				continue
			}
			if req.TemporalPolicy == TemporalPolicyCurrentOnly && !isCurrentVersion(msg, now) {
				continue
			}
			score := lexicalSearchScore(msg.Content, query)
			if score < threshold {
				continue
			}
			results = append(results, SearchResult{Message: msg, Score: score, Distance: 1 - score})
		}
	}
	sortSearchResults(results, req.TemporalPolicy, now)
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func lexicalSearchScore(content, normalizedQuery string) float32 {
	content = strings.ToLower(strings.TrimSpace(content))
	if content == normalizedQuery {
		return 1
	}
	if strings.Contains(content, normalizedQuery) {
		return 0.9
	}
	queryWords := keywordWords(normalizedQuery)
	if len(queryWords) == 0 {
		return 0
	}
	matches := 0
	for _, word := range queryWords {
		if strings.Contains(content, word) {
			matches++
		}
	}
	return float32(matches) / float32(len(queryWords))
}

func sortSearchResults(results []SearchResult, policy TemporalPolicy, now time.Time) {
	sort.SliceStable(results, func(i, j int) bool {
		if policy == TemporalPolicyCurrentFirst {
			currentI := isCurrentVersion(results[i].Message, now)
			currentJ := isCurrentVersion(results[j].Message, now)
			if currentI != currentJ {
				return currentI
			}
		}
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if !results[i].Message.Timestamp.Equal(results[j].Message.Timestamp) {
			return results[i].Message.Timestamp.After(results[j].Message.Timestamp)
		}
		return results[i].Message.ID < results[j].Message.ID
	})
}
