package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"data-producer-service/external/domain"

	"github.com/google/uuid"
)

func TestRawEventMessageJSON(t *testing.T) {
	msk := time.FixedZone("MSK", 3*60*60)
	p := domain.RawPackage{
		Id:           uuid.MustParse("6f1c2a3e-1b2c-4d5e-8f90-112233445566"),
		CreatedAt:    time.Date(2026, 10, 26, 12, 30, 0, 123456000, msk),
		RandomNumber: []int{7, 42},
	}

	got, err := json.Marshal(Map(&p))
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	const want = `{"id":"6f1c2a3e-1b2c-4d5e-8f90-112233445566","created_at":"2026-10-26T09:30:00.123456Z","random_number":[7,42]}`
	if string(got) != want {
		t.Errorf("json =\n%s\nwant\n%s", got, want)
	}
}
