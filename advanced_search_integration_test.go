package memory

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPostgresSearchMessagesFiltersBeforeCurrentFirstLimit(t *testing.T) {
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
	tag := fmt.Sprintf("search-test-%d", time.Now().UnixNano())
	filter := MessageFilter{ExtraEquals: map[string]interface{}{"search_test": tag}}
	t.Cleanup(func() {
		_, _ = mem.DeleteMessages(context.Background(), DeleteMessagesRequest{Filter: filter})
	})
	embedding := make([]float32, 1536)
	embedding[0] = 1
	first, err := mem.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "old", Embedding: embedding,
			Metadata: Metadata{SessionID: tag, Extra: map[string]interface{}{"search_test": tag}},
		},
		Namespace: tag, Key: "fact", Revision: "old",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := mem.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "current", Embedding: embedding,
			Metadata: Metadata{SessionID: tag, Extra: map[string]interface{}{"search_test": tag}},
		},
		Namespace: tag, Key: "fact", Revision: "current",
	})
	if err != nil {
		t.Fatal(err)
	}

	results, err := mem.SearchMessages(ctx, SearchMessagesRequest{
		Embedding: embedding, Threshold: 0.1, Limit: 1, Filter: filter,
		TemporalPolicy: TemporalPolicyCurrentFirst,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Message.ID != second.Message.ID {
		t.Fatalf("current-first results = %#v; old id = %s", results, first.Message.ID)
	}
	results, err = mem.SearchMessages(ctx, SearchMessagesRequest{
		Embedding: embedding, Threshold: 0.1, Limit: 10, Filter: filter,
		TemporalPolicy: TemporalPolicyCurrentOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Message.ID != second.Message.ID {
		t.Fatalf("current-only results = %#v", results)
	}
	results, err = mem.SearchMessages(ctx, SearchMessagesRequest{
		Query: "current", Mode: SearchModeKeyword, Limit: 10, Filter: filter,
		TemporalPolicy: TemporalPolicyCurrentOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Message.ID != second.Message.ID {
		t.Fatalf("keyword current-only results = %#v", results)
	}
}
