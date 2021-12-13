package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/GRVYDEV/migration"
	"github.com/GRVYDEV/migration/parser"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var postgresHost = os.Getenv("POSTGRES_HOST")

const database = "migrationtest"

func TestPostgresDriver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// prepare clean database
	connection, err := pgx.Connect(ctx, "postgres://postgres:@"+postgresHost+"/?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := connection.Close(ctx)
		if err != nil {
			t.Errorf("unexpected error while closing the postgres connection: %v", err)
		}
	}()

	_, err = connection.Exec(ctx, "CREATE DATABASE "+database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err := connection.Exec(ctx, "DROP DATABASE IF EXISTS "+database)
		if err != nil {
			t.Errorf("unexpected error while dropping the postgres database %s: %v", database, err)
		}
	}()

	connection2, err := pgx.Connect(ctx, "postgres://postgres:@"+postgresHost+"/"+database+"?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := connection2.Close(ctx)
		if err != nil {
			t.Errorf("unexpected error while closing the postgres connection: %v", err)
		}
	}()

	driver, err := New(ctx, "postgres://postgres:@"+postgresHost+"/"+database+"?sslmode=disable")
	if err != nil {
		t.Errorf("unable to open connection to postgres server: %s", err)
	}

	migrations := []*migration.PlannedMigration{
		{
			Migration: &migration.Migration{
				ID: "201610041422_init",
				Up: &parser.ParsedMigration{
					Statements: []string{
						`CREATE TABLE test_table1 (id integer not null primary key);

				   		 CREATE TABLE test_table2 (id integer not null primary key)`,
					},
					UseTransaction: false,
				},
			},
			Direction: migration.Up,
		},
		{
			Migration: &migration.Migration{
				ID: "201610041425_drop_unused_table",
				Up: &parser.ParsedMigration{
					Statements: []string{
						"DROP TABLE test_table2",
					},
					UseTransaction: false,
				},
				Down: &parser.ParsedMigration{
					Statements: []string{
						"CREATE TABLE test_table2(id integer not null primary key)",
					},
					UseTransaction: false,
				},
			},
			Direction: migration.Up,
		},
		{
			Migration: &migration.Migration{
				ID: "201610041422_invalid_sql",
				Up: &parser.ParsedMigration{
					Statements: []string{
						"CREATE TABLE test_table3 (some error",
					},
					UseTransaction: false,
				},
			},
			Direction: migration.Up,
		},
	}

	err = driver.Migrate(ctx, migrations[0])
	if err != nil {
		t.Errorf("unexpected error while running migration: %s", err)
	}

	_, err = connection2.Exec(ctx, "INSERT INTO test_table1 (id) values (1)")
	if err != nil {
		t.Errorf("unexpected error while testing if migration succeeded: %s", err)
	}

	_, err = connection2.Exec(ctx, "INSERT INTO test_table2 (id) values (1)")
	if err != nil {
		t.Errorf("unexpected error while testing if migration succeeded: %s", err)
	}

	err = driver.Migrate(ctx, migrations[1])
	if err != nil {
		t.Errorf("unexpected error while running migration: %s", err)
	}

	if _, err = connection2.Exec(ctx, "INSERT INTO test_table2 (id) values (1)"); err != nil {

		var pgxerr *pgconn.PgError

		if errors.As(err, &pgxerr) {
			if pgxerr.Code != "42P01" /* undefined_table */ {
				t.Errorf("received an error while inserting into a non-existent table, but it was not a undefined_table error: %s", err)
			}
		}
	} else {
		t.Error("expected an error while inserting into non-existent table, but did not receive any.")
	}

	err = driver.Migrate(ctx, migrations[2])
	if err == nil {
		t.Error("expected an error while executing invalid statement, but did not receive any.")
	}

	versions, err := driver.Versions(ctx)
	if err != nil {
		t.Errorf("unexpected error while retriving version information: %s", err)
	}
	if len(versions) != 2 {
		t.Errorf("expected %d versions to be applied, %d was actually applied.", 2, len(versions))
	}

	migrations[1].Direction = migration.Down

	err = driver.Migrate(ctx, migrations[1])
	if err != nil {
		t.Errorf("unexpected error while running migration: %s", err)
	}

	versions, err = driver.Versions(ctx)
	if err != nil {
		t.Errorf("unexpected error while retriving version information: %s", err)
	}
	if len(versions) != 1 {
		t.Errorf("expected %d versions to be applied, %d was actually applied.", 2, len(versions))
	}

	if err = driver.Close(ctx); err != nil {
		t.Errorf("unexpected error %v while closing the postgres driver.", err)
	}
}

func TestNewFromPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connection, err := pgx.Connect(ctx, "postgres://postgres:@"+postgresHost+"/?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := connection.Close(ctx)
		if err != nil {
			t.Errorf("unexpected error while closing the postgres connection: %v", err)
		}
	}()

	_, err = connection.Exec(ctx, "CREATE DATABASE "+database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err := connection.Exec(ctx, "DROP DATABASE IF EXISTS "+database)
		if err != nil {
			t.Errorf("unexpected error while dropping the postgres database %s: %v", database, err)
		}
	}()

	pool, err := pgxpool.Connect(ctx, "postgres://postgres:@"+postgresHost+"/"+database+"?sslmode=disable")
	if err != nil {
		t.Fatalf("error opening database pool: %s", err)
	}
	defer pool.Close()

	driver, err := NewFromPool(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := driver.Close(ctx)
		if err != nil {
			t.Errorf("unexpected error %v while closing the postgres driver from pool.", err)
		}
	}()

	versions, err := driver.Versions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(versions) != 0 {
		t.Errorf("expected empty version list, got %+v", versions)
	}
}
