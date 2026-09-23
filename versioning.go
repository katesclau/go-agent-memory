package memory

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	versionMetadataKey      = "_memory_version"
	versionStatusActive     = "active"
	versionStatusSuperseded = "superseded"
)

var (
	ErrVersionNamespaceRequired        = errors.New("version namespace is required")
	ErrVersionKeyRequired              = errors.New("version key is required")
	ErrVersionRevisionRequired         = errors.New("version revision is required")
	ErrVersionSessionRequired          = errors.New("versioned message session ID is required")
	ErrVersionEffectiveAtFuture        = errors.New("version effective time must not be in the future")
	ErrVersionEffectiveAtBeforeCurrent = errors.New("version effective time precedes the current version")
	ErrVersionSessionChanged           = errors.New("versioned messages must keep the same session ID")
)

type preparedVersionRequest struct {
	Message             Message
	Namespace           string
	Key                 string
	Revision            string
	EffectiveAt         time.Time
	ExplicitEffectiveAt bool
}

func prepareVersionRequest(req VersionedMessageRequest) (preparedVersionRequest, error) {
	namespace := strings.ToLower(strings.TrimSpace(req.Namespace))
	if namespace == "" {
		return preparedVersionRequest{}, ErrVersionNamespaceRequired
	}
	key := strings.ToLower(strings.TrimSpace(req.Key))
	if key == "" {
		return preparedVersionRequest{}, ErrVersionKeyRequired
	}
	revision := strings.TrimSpace(req.Revision)
	if revision == "" {
		return preparedVersionRequest{}, ErrVersionRevisionRequired
	}
	if strings.TrimSpace(req.Message.Metadata.SessionID) == "" {
		return preparedVersionRequest{}, ErrVersionSessionRequired
	}
	effectiveAt := req.EffectiveAt
	explicitEffectiveAt := !effectiveAt.IsZero()
	if effectiveAt.IsZero() {
		effectiveAt = time.Now()
	} else if effectiveAt.After(time.Now()) {
		return preparedVersionRequest{}, ErrVersionEffectiveAtFuture
	}
	return preparedVersionRequest{
		Message: req.Message, Namespace: namespace, Key: key,
		Revision: revision, EffectiveAt: effectiveAt.UTC(),
		ExplicitEffectiveAt: explicitEffectiveAt,
	}, nil
}

// VersionInfo returns typed version metadata without exposing how it is stored.
func VersionInfo(msg Message) (VersionMetadata, bool) {
	if msg.Metadata.Extra == nil {
		return VersionMetadata{}, false
	}
	raw, ok := msg.Metadata.Extra[versionMetadataKey]
	if !ok {
		return VersionMetadata{}, false
	}
	values, ok := raw.(map[string]interface{})
	if !ok {
		return VersionMetadata{}, false
	}
	namespace, _ := values["namespace"].(string)
	key, _ := values["key"].(string)
	revision, _ := values["revision"].(string)
	status, _ := values["status"].(string)
	supersedesID, _ := values["supersedes_id"].(string)
	version := versionNumber(values["version"])
	validFrom, _ := parseVersionTime(values["valid_from"])
	validUntil, _ := parseVersionTime(values["valid_until"])
	if namespace == "" || key == "" || revision == "" || version < 1 {
		return VersionMetadata{}, false
	}
	return VersionMetadata{
		Namespace: namespace, Key: key, Revision: revision, Version: version,
		Status: status, ValidFrom: validFrom, ValidUntil: validUntil,
		SupersedesID: supersedesID,
	}, true
}

func setVersionInfo(msg *Message, info VersionMetadata) {
	if msg.Metadata.Extra == nil {
		msg.Metadata.Extra = make(map[string]interface{})
	}
	values := map[string]interface{}{
		"namespace":  info.Namespace,
		"key":        info.Key,
		"revision":   info.Revision,
		"version":    info.Version,
		"status":     info.Status,
		"valid_from": info.ValidFrom.UTC().Format(time.RFC3339Nano),
	}
	if !info.ValidUntil.IsZero() {
		values["valid_until"] = info.ValidUntil.UTC().Format(time.RFC3339Nano)
	}
	if info.SupersedesID != "" {
		values["supersedes_id"] = info.SupersedesID
	}
	msg.Metadata.Extra[versionMetadataKey] = values
}

func supersedeMessage(msg *Message, at time.Time) {
	info, ok := VersionInfo(*msg)
	if !ok {
		return
	}
	info.Status = versionStatusSuperseded
	info.ValidUntil = at.UTC()
	setVersionInfo(msg, info)
}

func isCurrentVersion(msg Message, now time.Time) bool {
	info, ok := VersionInfo(msg)
	if !ok {
		return true
	}
	if strings.EqualFold(info.Status, versionStatusSuperseded) {
		return false
	}
	return info.ValidUntil.IsZero() || info.ValidUntil.After(now)
}

func versionedMessageID(namespace, key string, version int) string {
	sum := sha256.Sum256([]byte(namespace + "\n" + key + "\n" + strconv.Itoa(version)))
	return "vm_" + hex.EncodeToString(sum[:16])
}

func versionLockKey(namespace, key string) int64 {
	hasher := sha256.New()
	var length [8]byte
	for _, value := range []string{namespace, key} {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(value))
	}
	sum := hasher.Sum(nil)
	return int64(binary.BigEndian.Uint64(sum[:8]) >> 1)
}

func versionNumber(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		if v > int64(^uint(0)>>1) || v < 1 {
			return 0
		}
		return int(v)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Trunc(v) != v ||
			v < 1 || v > float64(^uint(0)>>1) {
			return 0
		}
		return int(v)
	case string:
		if !canonicalVersionNumber(v) {
			return 0
		}
		number, err := strconv.Atoi(v)
		if err != nil {
			return 0
		}
		return number
	default:
		return 0
	}
}

func canonicalVersionNumber(value string) bool {
	if value == "" || value[0] < '1' || value[0] > '9' {
		return false
	}
	for index := 1; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func parseVersionTime(value interface{}) (time.Time, bool) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.DateOnly} {
		parsed, err := time.Parse(layout, text)
		if err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func versionResult(msg Message, duplicate bool) (VersionedMessageResult, error) {
	info, ok := VersionInfo(msg)
	if !ok {
		return VersionedMessageResult{}, fmt.Errorf("message %q has invalid version metadata", msg.ID)
	}
	return VersionedMessageResult{
		Message: msg, Version: info.Version, Duplicate: duplicate,
		SupersededID: info.SupersedesID,
	}, nil
}
