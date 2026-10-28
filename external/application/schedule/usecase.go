package schedule

import (
	"context"
	"data-producer-service/external/domain"
	"fmt"
)

type Producer interface {
	Produce(ctx context.Context) (domain.RawPackage, error)
}
type EventSaver interface {
	Save(ctx context.Context, rawPackage domain.RawPackage) error
}

type UseCase struct {
	producer   Producer
	eventSaver EventSaver
}

func NewUseCase(producer Producer, eventStorage EventSaver) *UseCase {
	return &UseCase{
		producer:   producer,
		eventSaver: eventStorage,
	}
}

func (uc *UseCase) Schedule(ctx context.Context) error {
	rawPackage, err := uc.producer.Produce(ctx)
	if err != nil {
		return fmt.Errorf("produce raw package: %w", err)
	}
	if err := uc.eventSaver.Save(ctx, rawPackage); err != nil {
		return fmt.Errorf("save raw package %s: %w", rawPackage.Id, err)
	}
	return nil
}
