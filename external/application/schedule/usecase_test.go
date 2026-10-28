package schedule

import (
	"context"
	"data-producer-service/external/domain"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type FakeProducer struct {
	called     int
	err        error
	rawPackage domain.RawPackage
}

type FakeEventSaver struct {
	called     int
	err        error
	rawPackage domain.RawPackage
}

func (fp *FakeProducer) Produce(_ context.Context) (domain.RawPackage, error) {
	fp.called++
	return fp.rawPackage, fp.err
}

func (fe *FakeEventSaver) Save(_ context.Context, rawPackage domain.RawPackage) error {
	fe.called++
	fe.rawPackage = rawPackage
	return fe.err
}

func TestScheduleUseCase(t *testing.T) {
	u := uuid.New()
	now := time.Now()
	fp := &FakeProducer{
		err: nil,
		rawPackage: domain.RawPackage{
			Id:           u,
			CreatedAt:    now,
			RandomNumber: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}
	fe := &FakeEventSaver{
		err: nil,
	}
	uc := NewUseCase(fp, fe)
	err := uc.Schedule(t.Context())
	if err != nil {
		t.Errorf("should not return an error")
	}
	if fp.called != 1 {
		t.Errorf("fp.called should be 1")
	}
	if fe.called != 1 {
		t.Errorf("fe.called should be 1")
	}
	if fp.rawPackage.Id != fe.rawPackage.Id {
		t.Errorf("fp.rawPackage should be equal to fe.rawPackage")
	}
}

func TestScheduleProduceFails(t *testing.T) {
	errProduce := errors.New("producer is broken")
	fp := &FakeProducer{err: errProduce}
	fe := &FakeEventSaver{}

	err := NewUseCase(fp, fe).Schedule(t.Context())

	if !errors.Is(err, errProduce) {
		t.Fatalf("Schedule() error = %v, want it to wrap %v", err, errProduce)
	}
	if fe.called != 0 {
		t.Errorf("Save called %d times, want 0", fe.called)
	}
}
