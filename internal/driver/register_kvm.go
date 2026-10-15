//go:build kvm

package driver

import (
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/kvm"
)

func init() {
	Register("kvm", func() (core.Driver, error) { return kvm.New() })
}
