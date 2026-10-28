package producer

import (
	"context"
	"fmt"

	"data-producer-service/external/application/schedule"
	"data-producer-service/external/domain"
)

var _ schedule.Producer = (*RawPackageProducer)(nil)

type RawPackageProducer struct {
	randomNumberSize int
}

func NewRawPackageProducer(randomNumberSize int) (*RawPackageProducer, error) {
	if randomNumberSize < 1 {
		return nil, fmt.Errorf("random number size %d: %w", randomNumberSize, domain.InvalidRawPackageCount)
	}
	return &RawPackageProducer{randomNumberSize: randomNumberSize}, nil
}

func (p *RawPackageProducer) Produce(_ context.Context) (domain.RawPackage, error) {
	rawPackage, err := domain.NewRawPackage(p.randomNumberSize)
	if err != nil {
		return domain.RawPackage{}, fmt.Errorf("new raw package: %w", err)
	}
	return *rawPackage, nil
}
