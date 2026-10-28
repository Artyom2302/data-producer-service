package domain

import (
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

type RawPackage struct {
	Id           uuid.UUID
	CreatedAt    time.Time
	RandomNumber []int
}

func NewRawPackage(randomNumberCount int) (*RawPackage, error) {
	if randomNumberCount < 1 {
		return nil, InvalidRawPackageCount
	}
	RandomNumber := make([]int, randomNumberCount)
	for i := range randomNumberCount {
		RandomNumber[i] = rand.Int()
	}

	return &RawPackage{
		Id:           uuid.New(),
		CreatedAt:    time.Now(),
		RandomNumber: RandomNumber,
	}, nil
}
