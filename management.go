package memory

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"
)

var (
	ErrDeleteFilterRequired = errors.New("delete requires a filter or AllowAll")
	ErrInvalidTemporalState = errors.New("invalid temporal state")
	ErrInvalidMessageOrder  = errors.New("invalid message order")
	ErrInvalidPagination    = errors.New("limit and offset must not be negative")
)

func validateMessageFilter(filter MessageFilter) error {
	switch filter.TemporalState {
	case TemporalStateAny, TemporalStateCurrent, TemporalStateHistorical:
		return nil
	default:
		return ErrInvalidTemporalState
	}
}

func validateListRequest(req ListMessagesRequest) error {
	if err := validateMessageFilter(req.Filter); err != nil {
		return err
	}
	if req.Limit < 0 || req.Offset < 0 {
		return ErrInvalidPagination
	}
	switch req.Order {
	case "", MessageOrderNewest, MessageOrderOldest:
		return nil
	default:
		return ErrInvalidMessageOrder
	}
}

func emptyMessageFilter(filter MessageFilter) bool {
	return len(filter.MessageIDs) == 0 &&
		strings.TrimSpace(filter.SessionID) == "" &&
		strings.TrimSpace(filter.UserID) == "" &&
		len(filter.ExtraEquals) == 0 &&
		len(filter.ExtraNotEquals) == 0 &&
		len(filter.ExtraEqualFold) == 0 &&
		filter.CreatedAfter == nil &&
		filter.CreatedBefore == nil &&
		filter.TemporalState == TemporalStateAny
}

func messageMatchesFilter(msg Message, filter MessageFilter, now time.Time) bool {
	if len(filter.MessageIDs) > 0 && !containsString(filter.MessageIDs, msg.ID) {
		return false
	}
	if sessionID := strings.TrimSpace(filter.SessionID); sessionID != "" &&
		msg.Metadata.SessionID != sessionID {
		return false
	}
	if userID := strings.TrimSpace(filter.UserID); userID != "" &&
		msg.Metadata.UserID != userID {
		return false
	}
	for key, expected := range filter.ExtraEquals {
		if msg.Metadata.Extra == nil || !jsonSemanticEqual(msg.Metadata.Extra[key], expected) {
			return false
		}
	}
	for key, forbidden := range filter.ExtraNotEquals {
		value, exists := msg.Metadata.Extra[key]
		if exists && jsonSemanticEqual(value, forbidden) {
			return false
		}
	}
	for key, expected := range filter.ExtraEqualFold {
		value, ok := msg.Metadata.Extra[key].(string)
		if !ok || !strings.EqualFold(value, expected) {
			return false
		}
	}
	if filter.CreatedAfter != nil && !msg.Timestamp.After(*filter.CreatedAfter) {
		return false
	}
	if filter.CreatedBefore != nil && !msg.Timestamp.Before(*filter.CreatedBefore) {
		return false
	}
	switch filter.TemporalState {
	case TemporalStateCurrent:
		flatCurrent, _ := flatTemporalState(msg, now)
		if !isCurrentVersion(msg, now) || (filter.IncludeFlatTemporal && !flatCurrent) {
			return false
		}
	case TemporalStateHistorical:
		_, versioned := VersionInfo(msg)
		flatCurrent, flatVersioned := flatTemporalState(msg, now)
		current := isCurrentVersion(msg, now)
		if filter.IncludeFlatTemporal {
			current = current && flatCurrent
			versioned = versioned || flatVersioned
		}
		if !versioned || current {
			return false
		}
	}
	return true
}

func flatTemporalState(msg Message, now time.Time) (current, recognized bool) {
	if msg.Metadata.Extra == nil {
		return true, false
	}
	status, hasStatus := msg.Metadata.Extra["status"].(string)
	validUntil, hasValidUntil := msg.Metadata.Extra["valid_until"].(string)
	recognized = hasStatus || hasValidUntil
	if strings.EqualFold(strings.TrimSpace(status), versionStatusSuperseded) {
		return false, recognized
	}
	if !hasValidUntil || strings.TrimSpace(validUntil) == "" {
		return true, recognized
	}
	parsed, ok := parseVersionTime(validUntil)
	if !ok {
		return true, recognized
	}
	return parsed.After(now), recognized
}

func jsonSemanticEqual(left, right interface{}) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return reflect.DeepEqual(left, right)
	}
	return bytes.Equal(leftJSON, rightJSON)
}

func sortMessages(messages []Message, order MessageOrder) {
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].Timestamp.Equal(messages[j].Timestamp) {
			return messages[i].ID < messages[j].ID
		}
		if order == MessageOrderOldest {
			return messages[i].Timestamp.Before(messages[j].Timestamp)
		}
		return messages[i].Timestamp.After(messages[j].Timestamp)
	})
}

func paginateMessages(messages []Message, offset, limit int) []Message {
	if offset >= len(messages) {
		return []Message{}
	}
	messages = messages[offset:]
	if limit == 0 {
		limit = 100
	}
	if limit < len(messages) {
		messages = messages[:limit]
	}
	return messages
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
