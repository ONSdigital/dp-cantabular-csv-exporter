package generator

import (
	"crypto/rand"

	"github.com/google/uuid"
)

// Generator is responsible for randomly generating new strings and tokens
// that might need to be mocked out to produce consistent output for tests
type Generator struct{}

// New returns a new Generator
func New() *Generator {
	return &Generator{}
}

// NewUUID returns a new V4 unique ID
func (g *Generator) NewUUID() string {
	return uuid.NewString()
}

// NewPSK returns a new random array of 16 bytes
func (g *Generator) NewPSK() ([]byte, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}

	return key, nil
}
