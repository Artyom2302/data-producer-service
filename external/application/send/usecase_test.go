package send

import (
	"context"
	"errors"
	"slices"
	"testing"

	"data-producer-service/external/domain"

	"github.com/google/uuid"
)

type fakeSender struct {
	err   error
	calls int
	sent  []domain.RawPackage
}

func (f *fakeSender) Send(ctx context.Context, rawPackages []domain.RawPackage) error {
	f.calls++
	f.sent = append(f.sent, rawPackages...)
	return f.err
}

type fakeStorage struct {
	events     []domain.RawPackage
	receiveErr error
	gotLimit   int

	markErr   error
	markCalls int
	marked    []uuid.UUID
}

func (f *fakeStorage) Receive(_ context.Context, limitCount int) ([]domain.RawPackage, error) {
	f.gotLimit = limitCount
	return f.events, f.receiveErr
}

func (f *fakeStorage) MarkSent(_ context.Context, ids []uuid.UUID) error {
	f.markCalls++
	f.marked = append(f.marked, ids...)
	return f.markErr
}

func TestSendMarksSentPackages(t *testing.T) {
	const limit = 10
	a, b := uuid.New(), uuid.New()
	storage := &fakeStorage{events: []domain.RawPackage{{Id: a}, {Id: b}}}
	sender := &fakeSender{}

	uc, err := NewUseCase(limit, sender, storage)
	if err != nil {
		t.Fatalf("NewUseCase() error = %v", err)
	}

	n, err := uc.Send(t.Context())
	if err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	if n != 2 {
		t.Errorf("Send() = %d, want 2", n)
	}
	if storage.gotLimit != limit {
		t.Errorf("Receive got limit %d, want %d", storage.gotLimit, limit)
	}
	if len(sender.sent) != 2 {
		t.Errorf("sent %d packages, want 2", len(sender.sent))
	}
	if want := []uuid.UUID{a, b}; !slices.Equal(storage.marked, want) {
		t.Errorf("MarkSent got %v, want %v", storage.marked, want)
	}
}

func TestSendDoesNotMarkWhenSendFails(t *testing.T) {

	errKafka := errors.New("broker not available")
	storage := &fakeStorage{events: []domain.RawPackage{{Id: uuid.New()}}}
	sender := &fakeSender{err: errKafka}

	uc, err := NewUseCase(10, sender, storage)
	if err != nil {
		t.Fatalf("NewUseCase() error = %v", err)
	}
	n, err := uc.Send(t.Context())
	if !errors.Is(err, errKafka) {
		t.Fatalf("Send() error = %v, want it to wrap %v", err, errKafka)
	}

	if n != 0 {
		t.Errorf("Send() = %d, want 0", n)
	}
	if storage.markCalls != 0 {
		t.Errorf("MarkSent called %d times, want 0", storage.markCalls)
	}
}

func TestDoesNotMarkWhenMarkFailed(t *testing.T) {
	markErr := errors.New("mark failed")
	a, b := uuid.New(), uuid.New()
	storage := &fakeStorage{events: []domain.RawPackage{{Id: a}, {Id: b}}, markErr: markErr}
	sender := &fakeSender{err: nil}
	uc, err := NewUseCase(10, sender, storage)
	if err != nil {
		t.Fatalf("NewUseCase() error = %v", err)
	}
	n, err := uc.Send(t.Context())
	if !errors.Is(err, markErr) {
		t.Fatalf("Send() error = %v, want it to wrap %v", err, markErr)
	}
	if n != 2 {
		t.Errorf("Send() = %d, want 2", n)
	}
}
