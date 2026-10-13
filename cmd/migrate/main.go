package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 || (os.Args[1] != "up" && os.Args[1] != "down") {
		return fmt.Errorf("usage: go run ./cmd/migrate up|down")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connectionString := os.Getenv("DATABASE_URL")
	if connectionString == "" {
		connectionString = "postgres://postgres:postgres@localhost:5432/jungle_gaming?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(100) PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create migration tracking table: %w", err)
	}
	verb := os.Args[1]
	if verb == "up" {
		return applyPendingMigrations(ctx, pool)
	}
	return rollbackLatestMigration(ctx, pool)
}

func migrationVersions() ([]string, error) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	var versions []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		versions = append(versions, strings.TrimSuffix(name, ".up.sql"))
	}
	return versions, nil
}

func applyPendingMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	versions, err := migrationVersions()
	if err != nil {
		return err
	}
	for _, version := range versions {
		var applied bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&applied); err != nil {
			return fmt.Errorf("check migration state for %s: %w", version, err)
		}
		if applied {
			continue
		}
		if err := executeMigration(ctx, pool, version, "up"); err != nil {
			return err
		}
		fmt.Printf("migration %s: up\n", version)
	}
	return nil
}

func rollbackLatestMigration(ctx context.Context, pool *pgxpool.Pool) error {
	var version string
	if err := pool.QueryRow(ctx, `
		SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1
	`).Scan(&version); err != nil {
		if err == pgx.ErrNoRows {
			fmt.Println("no migration is applied")
			return nil
		}
		return fmt.Errorf("find latest applied migration: %w", err)
	}
	if err := executeMigration(ctx, pool, version, "down"); err != nil {
		return err
	}
	fmt.Printf("migration %s: down\n", version)
	return nil
}

func executeMigration(ctx context.Context, pool *pgxpool.Pool, version, direction string) error {
	path := filepath.Join("migrations", version+"."+direction+".sql")
	sql, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", path, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", version, err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, string(sql), pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("execute migration %s: %w", path, err)
	}
	if direction == "up" {
		_, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version)
	}
	if err != nil {
		return fmt.Errorf("update migration tracking: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
