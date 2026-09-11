package memory

import (
	"context"
	"fmt"
	"os"
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
			Metadata:  Metadata{SessionID: tag, Extra: map[string]interface{}{"test_tag": tag}},
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
	deleted, err := mem.DeleteMessages(ctx, DeleteMessagesRequest{Filter: filter})
	if err != nil || deleted != 2 {
		t.Fatalf("deleted = %d, err = %v", deleted, err)
	}
}
