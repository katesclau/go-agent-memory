package memory

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPostgresVersionedMessageLifecycle(t *testing.T) {
	databaseURL := os.Getenv("MEMORY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MEMORY_TEST_DATABASE_URL is not configured")
	}
	raw, err := NewSupabaseMemory(Config{
		DatabaseURL: databaseURL, VectorDimension: 1536,
	})
	if err != nil {
		t.Fatal(err)
	}
	mem := raw.(*SupabaseMemory)
	t.Cleanup(func() { _ = mem.Close() })

	ctx := context.Background()
	namespace := fmt.Sprintf("version-lifecycle-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = mem.db.Exec(context.Background(), `
			DELETE FROM agent_messages
			WHERE metadata->'extra'->'_memory_version'->>'namespace' = $1`,
			namespace,
		)
	})
	request := VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "seven seconds",
			Metadata:  Metadata{SessionID: namespace},
			Embedding: make([]float32, 1536),
		},
		Namespace: namespace, Key: "startup-timeout", Revision: "seven-seconds",
	}

	first, err := mem.PutVersionedMessage(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != 1 || first.Duplicate {
		t.Fatalf("first result = %#v", first)
	}

	retry, err := mem.PutVersionedMessage(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Message.ID != first.Message.ID || !retry.Duplicate || retry.Version != 1 {
		t.Fatalf("retry result = %#v", retry)
	}

	request.Message.Content = "nine seconds"
	request.Revision = "nine-seconds"
	second, err := mem.PutVersionedMessage(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != 2 || second.Duplicate || second.SupersededID != first.Message.ID {
		t.Fatalf("second result = %#v", second)
	}

	current, err := mem.currentVersion(ctx, namespace, request.Key, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if current.ID != second.Message.ID {
		t.Fatalf("current message = %q, want %q", current.ID, second.Message.ID)
	}
}

func TestPostgresVersionedMessageConcurrencyAndRollback(t *testing.T) {
	databaseURL := os.Getenv("MEMORY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MEMORY_TEST_DATABASE_URL is not configured")
	}
	raw, err := NewSupabaseMemory(Config{
		DatabaseURL: databaseURL, VectorDimension: 1536,
	})
	if err != nil {
		t.Fatal(err)
	}
	mem := raw.(*SupabaseMemory)
	t.Cleanup(func() { _ = mem.Close() })

	ctx := context.Background()
	namespace := fmt.Sprintf("version-test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = mem.db.Exec(context.Background(), `
			DELETE FROM agent_messages
			WHERE metadata->'extra'->'_memory_version'->>'namespace' = $1`,
			namespace,
		)
	})

	const writers = 12
	results := make(chan VersionedMessageResult, writers)
	errors := make(chan error, writers)
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(index int) {
			defer wg.Done()
			result, putErr := mem.PutVersionedMessage(ctx, VersionedMessageRequest{
				Message: Message{
					Role: "system", Content: fmt.Sprintf("value-%d", index),
					Metadata:  Metadata{SessionID: "session"},
					Embedding: make([]float32, 1536),
				},
				Namespace: namespace, Key: "key",
				Revision: fmt.Sprintf("revision-%d", index),
			})
			if putErr != nil {
				errors <- putErr
				return
			}
			results <- result
		}(i)
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Errorf("concurrent put: %v", err)
	}

	seen := make(map[int]bool, writers)
	for result := range results {
		seen[result.Version] = true
	}
	if len(seen) != writers {
		t.Fatalf("allocated versions = %v, want %d unique versions", seen, writers)
	}
	for version := 1; version <= writers; version++ {
		if !seen[version] {
			t.Fatalf("version %d was not allocated", version)
		}
	}

	current, err := mem.currentVersion(ctx, namespace, "key", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	currentInfo, ok := VersionInfo(current)
	if !ok || currentInfo.Version != writers {
		t.Fatalf("current version = %#v", currentInfo)
	}

	_, err = mem.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "must roll back",
			Metadata: Metadata{
				SessionID: "session",
				Extra:     map[string]interface{}{"invalid": make(chan int)},
			},
			Embedding: make([]float32, 1536),
		},
		Namespace: namespace, Key: "key", Revision: "rollback",
	})
	if err == nil {
		t.Fatal("expected metadata marshal failure")
	}

	afterFailure, err := mem.currentVersion(ctx, namespace, "key", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, _ := VersionInfo(afterFailure)
	if afterInfo.Version != writers || afterFailure.ID != current.ID {
		t.Fatalf("rollback changed current version from %#v to %#v", currentInfo, afterInfo)
	}
}

func TestHybridVersionedMessageInvalidatesSessionCache(t *testing.T) {
	databaseURL := os.Getenv("MEMORY_TEST_DATABASE_URL")
	redisAddr := os.Getenv("MEMORY_TEST_REDIS_ADDR")
	if databaseURL == "" || redisAddr == "" {
		t.Skip("MEMORY_TEST_DATABASE_URL and MEMORY_TEST_REDIS_ADDR are required")
	}
	raw, err := NewHybridMemory(Config{
		DatabaseURL: databaseURL, RedisAddr: redisAddr, VectorDimension: 1536,
	})
	if err != nil {
		t.Fatal(err)
	}
	mem := raw.(*HybridMemory)
	t.Cleanup(func() { _ = mem.Close() })
	ctx := context.Background()
	namespace := fmt.Sprintf("hybrid-version-test-%d", time.Now().UnixNano())
	embedding := make([]float32, 1536)
	embedding[0] = 1

	first, err := mem.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "old", Embedding: embedding,
			Metadata: Metadata{SessionID: namespace},
		},
		Namespace: namespace, Key: "key", Revision: "old",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.cacheMessage(ctx, first.Message); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "current", Embedding: embedding,
			Metadata: Metadata{SessionID: namespace},
		},
		Namespace: namespace, Key: "key", Revision: "current",
	}); err != nil {
		t.Fatal(err)
	}
	exists, err := mem.redis.Exists(ctx, fmt.Sprintf("session:%s:messages", namespace)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatal("versioned write left stale session messages in Redis")
	}
}
