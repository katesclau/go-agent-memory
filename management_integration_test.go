package memory

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPostgresManagedMemoryRoundTrip(t *testing.T) {
	databaseURL := os.Getenv("MEMORY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MEMORY_TEST_DATABASE_URL is not configured")
	}
	raw, err := NewSupabaseMemory(Config{DatabaseURL: databaseURL, VectorDimension: 1536})
	if err != nil {
		t.Fatal(err)
	}
	mem := raw.(*SupabaseMemory)
	t.Cleanup(func() { _ = mem.Close() })

	ctx := context.Background()
	tag := fmt.Sprintf("management-test-%d", time.Now().UnixNano())
	filter := MessageFilter{ExtraEquals: map[string]interface{}{"test_tag": tag}}
	t.Cleanup(func() {
		_, _ = mem.DeleteMessages(context.Background(), DeleteMessagesRequest{Filter: filter})
	})
	for index := 0; index < 2; index++ {
		embedding := make([]float32, 1536)
		embedding[0] = 1
		if err := mem.AddMessage(ctx, Message{
			ID: fmt.Sprintf("%s-%d", tag, index), Role: "system",
			Content: fmt.Sprintf("message-%d", index), Timestamp: time.Now().Add(time.Duration(index) * time.Second),
			Metadata:  Metadata{SessionID: tag, Extra: map[string]interface{}{"test_tag": tag, "number": 1}},
			Embedding: embedding,
		}); err != nil {
			t.Fatal(err)
		}
	}

	messages, err := mem.ListMessages(ctx, ListMessagesRequest{
		Filter: filter, Order: MessageOrderOldest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Content != "message-0" {
		t.Fatalf("messages = %#v", messages)
	}
	count, err := mem.CountMessages(ctx, filter)
	if err != nil || count != 2 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	count, err = mem.CountMessages(ctx, MessageFilter{
		ExtraEqualFold: map[string]string{"test_tag": strings.ToUpper(tag)},
	})
	if err != nil || count != 2 {
		t.Fatalf("case-folded count = %d, err = %v", count, err)
	}
	count, err = mem.CountMessages(ctx, MessageFilter{
		ExtraEquals: map[string]interface{}{"test_tag": tag, "number": float64(1)},
	})
	if err != nil || count != 2 {
		t.Fatalf("JSON numeric count = %d, err = %v", count, err)
	}

	for _, msg := range []Message{
		{
			ID: tag + "-malformed", Role: "system", Content: "malformed",
			Timestamp: time.Now(), Embedding: make([]float32, 1536),
			Metadata: Metadata{SessionID: tag, Extra: map[string]interface{}{
				"test_tag": tag,
				versionMetadataKey: map[string]interface{}{
					"namespace": tag, "key": "malformed", "version": 1,
					"status": versionStatusSuperseded,
				},
			}},
		},
		{
			ID: tag + "-expired", Role: "system", Content: "expired",
			Timestamp: time.Now(), Embedding: make([]float32, 1536),
			Metadata: Metadata{SessionID: tag, Extra: map[string]interface{}{
				"test_tag": tag,
				versionMetadataKey: map[string]interface{}{
					"namespace": tag, "key": "expired", "revision": "one", "version": 1,
					"status":      versionStatusActive,
					"valid_from":  time.Now().Add(-2 * time.Hour).Format(time.RFC3339Nano),
					"valid_until": time.Now().Add(-time.Hour).Format(time.RFC3339Nano),
				},
			}},
		},
		{
			ID: tag + "-overflow", Role: "system", Content: "overflow",
			Timestamp: time.Now(), Embedding: make([]float32, 1536),
			Metadata: Metadata{SessionID: tag, Extra: map[string]interface{}{
				"test_tag": tag,
				versionMetadataKey: map[string]interface{}{
					"namespace": tag, "key": "overflow", "revision": "one",
					"version": "999999999999999999999999999999999999",
					"status":  versionStatusSuperseded,
				},
			}},
		},
	} {
		if err := mem.AddMessage(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	current, err := mem.CountMessages(ctx, MessageFilter{
		ExtraEquals: filter.ExtraEquals, TemporalState: TemporalStateCurrent,
	})
	if err != nil || current != 4 {
		t.Fatalf("current count = %d, err = %v", current, err)
	}
	historical, err := mem.CountMessages(ctx, MessageFilter{
		ExtraEquals: filter.ExtraEquals, TemporalState: TemporalStateHistorical,
	})
	if err != nil || historical != 1 {
		t.Fatalf("historical count = %d, err = %v", historical, err)
	}
	deleted, err := mem.DeleteMessages(ctx, DeleteMessagesRequest{Filter: filter})
	if err != nil || deleted != 5 {
		t.Fatalf("deleted = %d, err = %v", deleted, err)
	}
}
