package request

// ContextKey is an alias of type string
type ContextKey string

// Context keys
const (
	UserIdentityKey        = ContextKey("User-Identity")
	CallerIdentityKey      = ContextKey("Caller-Identity")
	RequestIdKey           = ContextKey("request-id")
	FlorenceIdentityKey    = ContextKey("florence-id")
	LocaleContextKey       = ContextKey(LocaleHeaderKey)
	CollectionIDContextKey = ContextKey(CollectionIDHeaderKey)
)
