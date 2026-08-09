package store

import (
	"context"
	"embed"
	"fmt"
	"sort"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Migrate applies any unapplied SQL files from migrations/, in lexical
// order, each inside its own transaction.
//
// This is a deliberately tiny hand-rolled migrator (~60 lines) instead of
// goose/golang-migrate: Calligraphy needs exactly "apply these files once, in
// order, under a lock", and a dependency whose feature list is 95% unused
// is a dependency someone still has to be able to explain in review. The
// advisory lock serializes concurrent boots (two API replicas starting at
// once), which is the one concurrency hazard a migrator actually has.
func (s *Store) Migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("store: reading migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin migrate: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	// An arbitrary but fixed key: every Calligraphy process migrating this
	// database contends on the same lock. Transaction-scoped, so it
	// releases itself even if we die mid-way.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(72631001)`); err != nil {
		return fmt.Errorf("store: advisory lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("store: ensuring schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := tx.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("store: reading applied migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}

	for _, name := range names {
		if applied[name] {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("store: reading %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("store: applying %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			return fmt.Errorf("store: recording %s: %w", name, err)
		}
	}
	return tx.Commit(ctx)
}
