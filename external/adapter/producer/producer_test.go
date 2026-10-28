package producer

import (
	"errors"
	"testing"

	"data-producer-service/external/domain"

	"github.com/google/uuid"
)

func TestProduceCreatesPackageOfConfiguredSize(t *testing.T) {
	const size = 10
	producer, err := NewRawPackageProducer(size)
	if err != nil {
		t.Fatalf("NewRawPackageProducer(%d) error = %v", size, err)
	}

	produced, err := producer.Produce(t.Context())
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if produced.Id == uuid.Nil {
		t.Error("Produce() returned uuid.Nil Id")
	}
	if len(produced.RandomNumber) != size {
		t.Errorf("len(RandomNumber) = %d, want %d", len(produced.RandomNumber), size)
	}
}

func TestNewRawPackageProducerRejectsInvalidSize(t *testing.T) {
	for _, size := range []int{0, -1} {
		_, err := NewRawPackageProducer(size)
		if !errors.Is(err, domain.InvalidRawPackageCount) {
			t.Errorf("NewRawPackageProducer(%d) error = %v, want %v", size, err, domain.InvalidRawPackageCount)
		}
	}
}
