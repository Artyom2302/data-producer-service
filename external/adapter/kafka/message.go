package kafka

import (
	"data-producer-service/external/domain"
	"time"

	"github.com/google/uuid"
)

type RawEventMessage struct {
	Id           uuid.UUID `json:"id"`
	CreatedAt    time.Time `json:"created_at"`
	RandomNumber []int     `json:"random_number"`
}

func Map(p *domain.RawPackage) *RawEventMessage {
	return &RawEventMessage{
		Id:           p.Id,
		CreatedAt:    p.CreatedAt.UTC(),
		RandomNumber: p.RandomNumber,
	}
}
