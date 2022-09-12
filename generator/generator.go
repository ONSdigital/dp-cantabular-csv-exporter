package generator

import (
	"crypto/rand"
	"time"

	"github.com/hashicorp/go-uuid"
	"github.com/pkg/errors"
)

// Generator is responsible for randomly generating new strings and tokens
// that might need to be mocked out to produce consistent output for tests
type Generator struct{}

// New returns a new Generator
func New() *Generator {
	return &Generator{}
}

// NewPSK returns a new random array of 16 bytes
func (g *Generator) NewPSK() ([]byte, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}

	return key, nil
}

// Timestamp generates a timestamp of the current time
func (g *Generator) Timestamp() time.Time {
	return time.Now()
}

func (g *Generator) UniqueID() (string, error) {
	generateUUID, err := uuid.GenerateUUID()
	if err != nil {
		return "", errors.Wrap(err, "failed to generate an UUID for the S3 filename")
	}
	return generateUUID, nil
}
