package pubsub

import "time"

// Message is the message struct
type Message struct {
	ID          string
	Metadata    map[string]string
	Data        []byte
	PublishTime *time.Time
}
