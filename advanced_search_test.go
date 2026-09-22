package memory

import (
	"context"
	"testing"
)

func TestSessionOnlySearchMessagesCurrentFirst(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{
		MaxSessionMessages: 20, DefaultSearchLimit: 10, DefaultSearchThreshold: 0.1,
	})
	if err != nil {
		t.Fatal(err)
	}
	versioned := raw.(VersionedMemory)
	searchable := raw.(SearchableMemory)
	ctx := context.Background()

	first, err := versioned.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "timeout five",
			Metadata: Metadata{SessionID: "session", Extra: map[string]interface{}{"service": "api"}},
		},
		Namespace: "api", Key: "timeout", Revision: "five",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := versioned.PutVersionedMessage(ctx, VersionedMessageRequest{
		Message: Message{
			Role: "system", Content: "timeout ten",
			Metadata: Metadata{SessionID: "session", Extra: map[string]interface{}{"service": "api"}},
		},
		Namespace: "api", Key: "timeout", Revision: "ten",
	})
	if err != nil {
		t.Fatal(err)
	}

	all, err := searchable.SearchMessages(ctx, SearchMessagesRequest{
		Query: "timeout five", Threshold: 0.1, Limit: 10,
		Filter: MessageFilter{ExtraEquals: map[string]interface{}{"service": "api"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].Message.ID != first.Message.ID {
		t.Fatalf("all versions = %#v", all)
	}

	currentFirst, err := searchable.SearchMessages(ctx, SearchMessagesRequest{
		Query: "timeout five", Threshold: 0.1, Limit: 10,
		TemporalPolicy: TemporalPolicyCurrentFirst,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(currentFirst) != 2 || currentFirst[0].Message.ID != second.Message.ID {
		t.Fatalf("current first = %#v", currentFirst)
	}

	currentOnly, err := searchable.SearchMessages(ctx, SearchMessagesRequest{
		Query: "timeout", Threshold: 0.1, Limit: 10,
		TemporalPolicy: TemporalPolicyCurrentOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(currentOnly) != 1 || currentOnly[0].Message.ID != second.Message.ID {
		t.Fatalf("current only = %#v", currentOnly)
	}
}

func TestSessionOnlySearchTreatsMalformedVersionMetadataAsCurrent(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{MaxSessionMessages: 10, DefaultSearchThreshold: 0.1})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.AddMessage(context.Background(), Message{
		ID: "legacy", Role: "system", Content: "legacy timeout",
		Metadata: Metadata{
			SessionID: "session",
			Extra: map[string]interface{}{
				versionMetadataKey: map[string]interface{}{"valid_until": "not-a-time"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	results, err := raw.(SearchableMemory).SearchMessages(context.Background(), SearchMessagesRequest{
		Query: "legacy", Threshold: 0.1, TemporalPolicy: TemporalPolicyCurrentOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %#v", results)
	}
}

func TestSessionOnlyKeywordSearchRanksOverlapAndAppliesFilters(t *testing.T) {
	raw, err := NewSessionOnlyMemory(Config{
		MaxSessionMessages: 10, DefaultSearchLimit: 5, DefaultSearchThreshold: 0.1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []Message{
		{
			ID: "one", Role: "system", Content: "timeout",
			Metadata: Metadata{SessionID: "session"},
		},
		{
			ID: "two", Role: "system", Content: "startup timeout",
			Metadata: Metadata{SessionID: "session"},
		},
		{
			ID: "document", Role: "system", Content: "startup timeout",
			Metadata: Metadata{
				SessionID: "session",
				Extra:     map[string]interface{}{"record_type": "document_chunk"},
			},
		},
		{
			ID: "unrelated", Role: "system", Content: "database retries",
			Metadata: Metadata{SessionID: "session"},
		},
	} {
		if err := raw.AddMessage(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	results, err := raw.(SearchableMemory).SearchMessages(context.Background(), SearchMessagesRequest{
		Query: "startup timeout?", Mode: SearchModeKeyword, Limit: 10, Threshold: 0.95,
		Filter: MessageFilter{
			ExtraNotEquals: map[string]interface{}{"record_type": "document_chunk"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Message.ID != "two" || results[1].Message.ID != "one" {
		t.Fatalf("results = %#v", results)
	}
}
