package domain

import (
	"errors"
)

var (
	InvalidRawPackageCount = errors.New("random number count must be greater than zero")
)
