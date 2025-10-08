package xbus

// Decode is a helper to unmarshal a message payload into a typed value using the provided codec.
func Decode[T any](c Codec, msg *Message) (T, error) {
	var v T
	if err := c.Unmarshal(msg.Payload, &v); err != nil {
		return v, err
	}
	return v, nil
}
