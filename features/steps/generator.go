package steps

import (
	"time"
)

const (
	testTimestamp = "2022-01-26T12:27:04.783936865Z"
	testPSK       = "0123456789ABCDEF"
)

// Generator is responsible for randomly generating new strings and tokens
// that might need to be mocked out to produce consistent output for tests
type generator struct{}

// NewPSK returns a new non-random array of 16 bytes
func (g *generator) NewPSK() ([]byte, error) {
	return []byte(testPSK), nil
}

// Timestamp generates a constant timestamp
func (g *generator) Timestamp() time.Time {
	t, _ := time.Parse(time.RFC3339, testTimestamp)
	return t
}
