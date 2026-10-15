//go:build !kvm

package driver

import (
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/fake"
)

func init() {
	Register("fake", func() (core.Driver, error) { return fake.New(), nil })
}
