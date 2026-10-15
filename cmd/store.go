package cmd

import (
	"fmt"
	"strings"

	"github.com/cinvat/arsan/internal/app"
	"github.com/cinvat/arsan/internal/store/gormdb"
	"github.com/cinvat/arsan/internal/store/memory"
	"github.com/spf13/viper"
)

// openStore connects the shared database. Empty means in-memory, which is for
// development and tests only: two processes with two memory stores cannot see
// each other, so the api would write rows the controller never reads.
//
// A value starting with postgres:// opens PostgreSQL; anything else is a
// SQLite file both processes open. SQLite carries a busy timeout and WAL mode
// for exactly this: two writers, one file.
func openStore() (app.Stores, error) {
	dsn := viper.GetString("db")
	if dsn == "" {
		return memory.New(), nil
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return gormdb.OpenPostgres(dsn)
	}
	return gormdb.OpenSQLite(dsn)
}

func mustStore() (app.Stores, error) {
	s, err := openStore()
	if err != nil {
		return nil, fmt.Errorf("open store %q: %w", viper.GetString("db"), err)
	}
	return s, nil
}
