package steps

import (
	"crypto/rand"
)

const (
	testUUID = "66e23ec3-22fb-4a08-91bb-052ac37eb410"
)

// Generator is responsible for randomly generating new strings and tokens
// that might need to be mocked out to produce consistent output for tests
type generator struct{}

// NewUUID returns a new V4 unique ID
func (g *generator) NewUUID() string {
	return testUUID
}

// NewPSK returns a new random array of 16 bytes
func (g *generator) NewPSK() ([]byte, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}

	return key, nil
}
