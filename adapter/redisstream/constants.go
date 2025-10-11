package redisstream

// Field constants (avoid typos/allocs)
const (
	fieldID         = "id"
	fieldName       = "name"
	fieldPayload    = "payload"    // raw []byte to reduce allocs (no base64)
	fieldProducedAt = "producedAt" // int64 ns
	fieldMetaPrefix = "meta:"
)
