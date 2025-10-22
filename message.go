package pubsub

import "time"

// FieldKey is the field name which is used to store the message metadata.
const (
	FieldKey         = "key"
	FieldTag         = "tag"
	FieldShardingKey = "shardingKey"
)

// Message is the message struct
type Message struct {
	ID          string
	Metadata    map[string]string
	Data        []byte
	PublishTime *time.Time
}
