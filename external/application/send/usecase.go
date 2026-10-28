package send

import (
	"context"
	"data-producer-service/external/domain"
	"fmt"

	"github.com/google/uuid"
)

type EventSender interface {
	Send(ctx context.Context, rawPackages []domain.RawPackage) error
}

type EventStorage interface {
	Receive(ctx context.Context, limitCount int) ([]domain.RawPackage, error)
	MarkSent(ctx context.Context, ids []uuid.UUID) error
}

type UseCase struct {
	sentLimit    int
	eventSender  EventSender
	eventStorage EventStorage
}

func NewUseCase(sentLimit int, eventSender EventSender, eventStorage EventStorage) (*UseCase, error) {
	if sentLimit < 1 {
		return nil, fmt.Errorf("sentLimit must be greater or equal than 1: %d", sentLimit)
	}
	return &UseCase{
		sentLimit:    sentLimit,
		eventSender:  eventSender,
		eventStorage: eventStorage,
	}, nil
}

func (uc *UseCase) Send(ctx context.Context) (int, error) {
	events, err := uc.eventStorage.Receive(ctx, uc.sentLimit)
	if err != nil {
		return 0, fmt.Errorf("get events error: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}
	if err := uc.eventSender.Send(ctx, events); err != nil {
		return 0, fmt.Errorf("send events error: %w", err)
	}
	uuids := make([]uuid.UUID, 0, len(events))
	for _, e := range events {
		uuids = append(uuids, e.Id)
	}

	if err := uc.eventStorage.MarkSent(ctx, uuids); err != nil {
		return len(uuids), fmt.Errorf("mark sent error: %w", err)
	}

	return len(uuids), nil
}
