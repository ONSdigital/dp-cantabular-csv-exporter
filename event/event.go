package event

// InstanceComplete provides an avro structure for a Instance Complete event
type InstanceComplete struct {
	InstanceId string `avro:"instance_id"`
}
