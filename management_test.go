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
