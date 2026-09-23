package memory

import (
	"testing"

	"github.com/sashabaranov/go-openai"
)

func TestOpenAIClientConfig(t *testing.T) {
	t.Run("uses SDK default when base URL is empty", func(t *testing.T) {
		config, err := openAIClientConfig("test-key", "")
		if err != nil {
			t.Fatalf("openAIClientConfig returned error: %v", err)
		}
		if config.BaseURL != openai.DefaultConfig("test-key").BaseURL {
			t.Fatalf("BaseURL = %q, want SDK default", config.BaseURL)
		}
	})

	t.Run("accepts regional HTTPS endpoint and removes trailing slash", func(t *testing.T) {
		config, err := openAIClientConfig("test-key", " https://us.api.openai.com/v1/ ")
		if err != nil {
			t.Fatalf("openAIClientConfig returned error: %v", err)
		}
		if config.BaseURL != "https://us.api.openai.com/v1" {
			t.Fatalf("BaseURL = %q, want regional endpoint", config.BaseURL)
		}
	})

	for _, baseURL := range []string{
		"http://api.openai.com/v1",
		"https://",
		"api.openai.com/v1",
		"https://user:password@api.openai.com/v1",
		"https://api.openai.com/v1?region=us",
		"https://api.openai.com/v1#fragment",
	} {
		t.Run("rejects "+baseURL, func(t *testing.T) {
			if _, err := openAIClientConfig("test-key", baseURL); err == nil {
				t.Fatalf("openAIClientConfig(%q) returned no error", baseURL)
			}
		})
	}
}
