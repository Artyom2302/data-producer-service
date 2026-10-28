package postgres

import (
	"context"
	"data-producer-service/external/domain"
	"data-producer-service/migrations"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

func newTestStorage(t *testing.T) *PgStorage {
	t.Helper()

	dsn := os.Getenv("DSN_URL")
	if dsn == "" {
		t.Skip("DSN_URL is not set: skipping Postgres integration test")
	}
	if err := Migrate(t.Context(), dsn, migrations.MigrationFiles); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	storage, err := NewPgStorage(t.Context(), dsn)
	if err != nil {
		t.Fatalf("NewPgStorage() error = %v", err)
	}
	t.Cleanup(storage.Close)

	clean := func(ctx context.Context) {
		if _, err := storage.pool.Exec(ctx, `delete from packages`); err != nil {
			t.Errorf("clean packages: %v", err)
		}
	}
	clean(t.Context())
	t.Cleanup(func() { clean(context.Background()) })

	return storage
}

func TestReceiveReturnsOldestUnsent(t *testing.T) {
	storage := newTestStorage(t)

	base := time.Now().Truncate(time.Microsecond)
	saved := make([]domain.RawPackage, 3)
	for i := range saved {
		saved[i] = domain.RawPackage{
			Id:           uuid.New(),
			CreatedAt:    base.Add(time.Duration(i) * time.Second),
			RandomNumber: []int{i, i * 10},
		}
		if err := storage.Save(t.Context(), saved[i]); err != nil {
			t.Fatalf("Save(%d) error = %v", i, err)
		}
	}

	got, err := storage.Receive(t.Context(), 2)
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Receive() returned %d packages, want 2", len(got))
	}

	for i, want := range saved[:2] {
		if got[i].Id != want.Id {
			t.Errorf("got[%d].Id = %s, want %s", i, got[i].Id, want.Id)
		}
		if !got[i].CreatedAt.Equal(want.CreatedAt) {
			t.Errorf("got[%d].CreatedAt = %v, want %v", i, got[i].CreatedAt, want.CreatedAt)
		}
		if !slices.Equal(got[i].RandomNumber, want.RandomNumber) {
			t.Errorf("got[%d].RandomNumber = %v, want %v", i, got[i].RandomNumber, want.RandomNumber)
		}
	}
}

func TestMarkSentHidesPackagesFromReceive(t *testing.T) {
	storage := newTestStorage(t)

	base := time.Now().Truncate(time.Microsecond)
	saved := make([]domain.RawPackage, 3)
	for i := range saved {
		saved[i] = domain.RawPackage{
			Id:           uuid.New(),
			CreatedAt:    base.Add(time.Duration(i) * time.Second),
			RandomNumber: []int{i},
		}
		if err := storage.Save(t.Context(), saved[i]); err != nil {
			t.Fatalf("Save(%d) error = %v", i, err)
		}
	}

	if err := storage.MarkSent(t.Context(), []uuid.UUID{saved[0].Id, saved[1].Id}); err != nil {
		t.Fatalf("MarkSent() error = %v", err)
	}

	got, err := storage.Receive(t.Context(), 10)
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if len(got) != 1 || got[0].Id != saved[2].Id {
		t.Fatalf("Receive() after MarkSent = %v, want only %s", got, saved[2].Id)
	}
}
