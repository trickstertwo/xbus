package xbus

import "context"

// PublishEvent describes a single event in a batch publish call.
type PublishEvent struct {
	Name    string
	Payload any
	Meta    map[string]string
}

// PublishBatch encodes and sends multiple events to a topic atomically per transport call.
func (b *Bus) PublishBatch(ctx context.Context, topic string, events ...PublishEvent) error {
	if len(events) == 0 {
		return nil
	}

	msgs := make([]*Message, 0, len(events))
	for i := range events {
		data, err := b.codec.Marshal(events[i].Payload)
		if err != nil {
			return err
		}
		msgs = append(msgs, &Message{
			Name:       events[i].Name,
			Payload:    data,
			Metadata:   events[i].Meta,
			ProducedAt: b.clock.Now(),
		})
	}

	for i := range msgs {
		b.notify(BusEvent{Type: EventPublishStart, Topic: topic, EventName: msgs[i].Name})
	}
	err := b.transport.Publish(ctx, topic, msgs...)
	for i := range msgs {
		b.notify(BusEvent{Type: EventPublishDone, Topic: topic, EventName: msgs[i].Name, Err: err})
	}
	return err
}
