package event

// InstanceComplete provides an avro structure for a Instance Complete event
type InstanceComplete struct {
	InstanceID string `avro:"instance_id"`
}
