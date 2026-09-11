package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSessionOnlyVersionedMessageLifecycle(t *testing.T) {
	mem, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 20})
	if err != nil {
		t.Fatal(err)
	}
	versioned := mem.(VersionedMemory)
	ctx := context.Background()

	first := putTestVersion(t, ctx, versioned, "one", "revision-1")
	if first.Version != 1 || first.Duplicate {
		t.Fatalf("first result = %#v", first)
	}
	retry := putTestVersion(t, ctx, versioned, "one", "revision-1")
	if retry.Message.ID != first.Message.ID || !retry.Duplicate {
		t.Fatalf("retry result = %#v", retry)
	}
	second := putTestVersion(t, ctx, versioned, "two", "revision-2")
	if second.Version != 2 || second.SupersededID != first.Message.ID {
		t.Fatalf("second result = %#v", second)
	}
	third := putTestVersion(t, ctx, versioned, "one", "revision-3")
	if third.Version != 3 || third.Message.ID == first.Message.ID {
		t.Fatalf("A-B-A result = %#v", third)
	}

	messages, err := mem.GetRecentMessages(ctx, "session", 20)
	if err != nil {
		t.Fatal(err)
	}
	current := 0
	for _, msg := range messages {
		if isCurrentVersion(msg, time.Now()) {
			current++
		}
	}
	if current != 1 {
		t.Fatalf("current versions = %d, want 1", current)
	}
}

func TestSessionOnlyVersionedMessageNamespacesAreIsolated(t *testing.T) {
	mem, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 20})
	if err != nil {
		t.Fatal(err)
	}
	versioned := mem.(VersionedMemory)
	first := putTestVersionInNamespace(t, context.Background(), versioned, "one", "revision", "a")
	second := putTestVersionInNamespace(t, context.Background(), versioned, "two", "revision", "b")
	if first.Version != 1 || second.Version != 1 {
		t.Fatalf("versions = %d and %d, want isolated version 1", first.Version, second.Version)
	}
}

func TestSessionOnlyVersionedMessageConcurrentWriters(t *testing.T) {
	mem, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 100})
	if err != nil {
		t.Fatal(err)
	}
	versioned := mem.(VersionedMemory)
	const writers = 20
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			_, putErr := versioned.PutVersionedMessage(context.Background(), VersionedMessageRequest{
				Message: Message{
					Role: "system", Content: "value",
					Metadata: Metadata{SessionID: "session"},
				},
				Namespace: "namespace", Key: "key", Revision: "same-revision",
			})
			if putErr != nil {
				t.Errorf("put version: %v", putErr)
			}
		}()
	}
	wg.Wait()

	messages, err := mem.GetRecentMessages(context.Background(), "session", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(messages))
	}
}

func TestSessionOnlyRejectsFutureAndBackdatedVersions(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 20})
	if err != nil {
		t.Fatal(err)
	}
	versioned := raw.(VersionedMemory)
	request := VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "value",
			Metadata: Metadata{SessionID: "session"},
		},
		Namespace: "namespace", Key: "key", Revision: "first",
		EffectiveAt: time.Now().Add(-time.Hour),
	}
	if _, err := versioned.PutVersionedMessage(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	request.Revision = "backdated"
	request.EffectiveAt = time.Now().Add(-2 * time.Hour)
	if _, err := versioned.PutVersionedMessage(context.Background(), request); !errors.Is(err, ErrVersionEffectiveAtBeforeCurrent) {
		t.Fatalf("backdated error = %v", err)
	}
	request.Revision = "future"
	request.EffectiveAt = time.Now().Add(time.Hour)
	if _, err := versioned.PutVersionedMessage(context.Background(), request); !errors.Is(err, ErrVersionEffectiveAtFuture) {
		t.Fatalf("future error = %v", err)
	}
}

func putTestVersion(
	t *testing.T,
	ctx context.Context,
	mem VersionedMemory,
	content, revision string,
) VersionedMessageResult {
	t.Helper()
	return putTestVersionInNamespace(t, ctx, mem, content, revision, "namespace")
}

func putTestVersionInNamespace(
	t *testing.T,
	ctx context.Context,
	mem VersionedMemory,
	content, revision, namespace string,
) VersionedMessageResult {
	t.Helper()
	result, err := mem.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: content,
			Metadata: Metadata{SessionID: "session"},
		},
		Namespace: namespace, Key: "key", Revision: revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
