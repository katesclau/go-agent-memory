package memory

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

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
