package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCreateRawPackage(t *testing.T) {
	const count = 10
	before := time.Now()
	rawPackage, err := NewRawPackage(count)
	after := time.Now()
	if err != nil {
		t.Fatalf("error occurred: %v", err)
	}
	if rawPackage == nil {
		t.Fatal("rawPackage is nil")
	}

	if len(rawPackage.RandomNumber) != count {
		t.Errorf("len(RandomNumber) = %d, want %d", len(rawPackage.RandomNumber), count)
	}
	if rawPackage.Id == uuid.Nil {
		t.Error("Id is uuid.Nil")
	}
	if rawPackage.CreatedAt.Before(before) || rawPackage.CreatedAt.After(after) {
		t.Errorf("rawPackage.CreatedAt = %v, want between %v and %v", rawPackage.CreatedAt, before, after)
	}
}

func TestCreateRawPackageWithWrongRandomNumber(t *testing.T) {
	tests := []struct {
		name         string
		randomNumber int
	}{
		{name: "zero", randomNumber: 0},
		{name: "negative", randomNumber: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rawPackage, err := NewRawPackage(test.randomNumber)
			if rawPackage != nil {
				t.Fatalf("rawPackage should be nil")
			}
			if !errors.Is(err, InvalidRawPackageCount) {
				t.Errorf("NewRawPackage(%d) return error %v, want %v", test.randomNumber, err, InvalidRawPackageCount)
			}
		})
	}
}
