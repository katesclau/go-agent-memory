package memory

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSessionOnlyManagedMemoryFiltersOrdersAndPaginates(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 20})
	if err != nil {
		t.Fatal(err)
	}
	managed := raw.(ManagedMemory)
	base := time.Now().Add(-time.Hour)
	for index, msg := range []Message{
		{ID: "one", Role: "system", Content: "one", Timestamp: base,
			Metadata: Metadata{SessionID: "a", UserID: "user", Extra: map[string]interface{}{"tag": "x"}}},
		{ID: "two", Role: "system", Content: "two", Timestamp: base.Add(time.Minute),
			Metadata: Metadata{SessionID: "a", UserID: "user", Extra: map[string]interface{}{"tag": "x"}}},
		{ID: "three", Role: "system", Content: "three", Timestamp: base.Add(2 * time.Minute),
			Metadata: Metadata{SessionID: "b", UserID: "other", Extra: map[string]interface{}{"tag": "y"}}},
	} {
		if err := raw.AddMessage(context.Background(), msg); err != nil {
			t.Fatalf("add message %d: %v", index, err)
		}
	}

	messages, err := managed.ListMessages(context.Background(), ListMessagesRequest{
		Filter: MessageFilter{
			SessionID: "a", UserID: "user",
			ExtraEquals: map[string]interface{}{"tag": "x"},
		},
		Order: MessageOrderOldest, Offset: 1, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].ID != "two" {
		t.Fatalf("messages = %#v", messages)
	}
	count, err := managed.CountMessages(context.Background(), MessageFilter{
		ExtraEquals: map[string]interface{}{"tag": "x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	count, err = managed.CountMessages(context.Background(), MessageFilter{
		ExtraEqualFold: map[string]string{"tag": "X"},
	})
	if err != nil || count != 2 {
		t.Fatalf("case-folded count = %d, err = %v", count, err)
	}
}

func TestSessionOnlyManagedMemoryTemporalStateAndDeleteGuard(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 20})
	if err != nil {
		t.Fatal(err)
	}
	versioned := raw.(VersionedMemory)
	managed := raw.(ManagedMemory)
	putTestVersion(t, context.Background(), versioned, "old", "old")
	putTestVersion(t, context.Background(), versioned, "current", "current")

	current, err := managed.CountMessages(context.Background(), MessageFilter{
		TemporalState: TemporalStateCurrent,
	})
	if err != nil {
		t.Fatal(err)
	}
	historical, err := managed.CountMessages(context.Background(), MessageFilter{
		TemporalState: TemporalStateHistorical,
	})
	if err != nil {
		t.Fatal(err)
	}
	if current != 1 || historical != 1 {
		t.Fatalf("current/historical = %d/%d, want 1/1", current, historical)
	}

	if _, err := managed.DeleteMessages(context.Background(), DeleteMessagesRequest{}); !errors.Is(err, ErrDeleteFilterRequired) {
		t.Fatalf("delete without filter error = %v", err)
	}
	deleted, err := managed.DeleteMessages(context.Background(), DeleteMessagesRequest{
		Filter: MessageFilter{TemporalState: TemporalStateHistorical},
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
}

func TestSessionOnlyManagedMemoryUsesJSONEquality(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.AddMessage(context.Background(), Message{
		ID: "json", Role: "system", Content: "json",
		Metadata: Metadata{
			SessionID: "session",
			Extra: map[string]interface{}{
				"number": 1,
				"nested": map[string]interface{}{"values": []interface{}{"a", float64(2)}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	count, err := raw.(ManagedMemory).CountMessages(context.Background(), MessageFilter{
		ExtraEquals: map[string]interface{}{
			"number": float64(1),
			"nested": map[string]interface{}{"values": []interface{}{"a", 2}},
		},
	})
	if err != nil || count != 1 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
}

func TestSessionOnlyTreatsOverflowingVersionAsLegacyCurrent(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.AddMessage(context.Background(), Message{
		ID: "overflow", Role: "system", Content: "legacy",
		Metadata: Metadata{SessionID: "session", Extra: map[string]interface{}{
			versionMetadataKey: map[string]interface{}{
				"namespace": "namespace", "key": "key", "revision": "revision",
				"version": "999999999999999999999999999999999999",
				"status":  versionStatusSuperseded,
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	current, err := raw.(ManagedMemory).CountMessages(context.Background(), MessageFilter{
		TemporalState: TemporalStateCurrent,
	})
	if err != nil || current != 1 {
		t.Fatalf("current count = %d, err = %v", current, err)
	}
}

func TestSessionOnlyExtraNotEqualsKeepsMissingMetadata(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []Message{
		{
			ID: "legacy", Role: "system", Content: "legacy",
			Metadata: Metadata{SessionID: "session"},
		},
		{
			ID: "document", Role: "system", Content: "document",
			Metadata: Metadata{
				SessionID: "session",
				Extra:     map[string]interface{}{"record_type": "document_chunk"},
			},
		},
		{
			ID: "expired", Role: "system", Content: "expired",
			Metadata: Metadata{
				SessionID: "session",
				Extra: map[string]interface{}{
					"valid_until": time.Now().Add(-time.Hour).Format(time.RFC3339Nano),
				},
			},
		},
	} {
		if err := raw.AddMessage(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := raw.(ManagedMemory).ListMessages(context.Background(), ListMessagesRequest{
		Filter: MessageFilter{
			ExtraNotEquals:      map[string]interface{}{"record_type": "document_chunk"},
			TemporalState:       TemporalStateCurrent,
			IncludeFlatTemporal: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].ID != "legacy" {
		t.Fatalf("messages = %#v", messages)
	}
}
