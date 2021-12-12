package postgres

import (
	"context"
	"fmt"

	m "github.com/GRVYDEV/migration"
	"github.com/GRVYDEV/migration/parser"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Driver is the postgres migration.Driver implementation
type Driver struct {
	db pgxQueryable
}

// pgxQueryable is an interface encapsulating the common methods required from
// either pgx.Conn or pgxpool.Pool.
type pgxQueryable interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error)
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

const postgresTableName = "schema_migration"

// New creates a new Driver and initializes a connection to the database. The
// context can be used to cancel the connection attempt.
// The DSN is documented here: https://pkg.go.dev/github.com/jackc/pgx/v4@v4.10.1/stdlib#pkg-overview
func New(ctx context.Context, dsn string) (m.Driver, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return NewFromConn(ctx, conn)
}

// NewFromConn creates a new Driver and initializes a connection to the database. The
// context can be used to cancel the connection attempt.
// The DSN is documented here: https://pkg.go.dev/github.com/jackc/pgx/v4@v4.10.1/stdlib#pkg-overview
func NewFromConn(ctx context.Context, conn *pgx.Conn) (m.Driver, error) {
	return newFromQueryable(ctx, conn)
}

// NewFromConn creates a new Driver and initializes a connection to the database. The
// context can be used to cancel the connection attempt.
// The DSN is documented here: https://pkg.go.dev/github.com/jackc/pgx/v4@v4.10.1/stdlib#pkg-overview
func newFromQueryable(ctx context.Context, q pgxQueryable) (m.Driver, error) {
	if err := q.Ping(ctx); err != nil {
		return nil, err
	}

	d := &Driver{
		db: q,
	}
	if err := d.ensureVersionTableExists(ctx); err != nil {
		return nil, err
	}

	return d, nil
}

type poolWithoutContextClose struct {
	*pgxpool.Pool
}

func (p *poolWithoutContextClose) Close(ctx context.Context) error {
	p.Pool.Close()
	return nil
}

// NewFromPool creates a new Driver and initializes a connection to the database. The
// context can be used to cancel the connection attempt.
// The DSN is documented here: https://pkg.go.dev/github.com/jackc/pgx/v4@v4.10.1/stdlib#pkg-overview
func NewFromPool(ctx context.Context, pool *pgxpool.Pool) (m.Driver, error) {
	return newFromQueryable(ctx, &poolWithoutContextClose{pool})
}

// Close closes the connection to the Driver server.
func (driver *Driver) Close(ctx context.Context) error {
	return driver.db.Close(ctx)
}

func (driver *Driver) ensureVersionTableExists(ctx context.Context) error {
	_, err := driver.db.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+postgresTableName+" (version varchar(255) not null primary key)")
	return err
}

// Migrate runs a migration.
func (driver *Driver) Migrate(ctx context.Context, migration *m.PlannedMigration) (err error) {
	var (
		migrationStatements *parser.ParsedMigration
		insertVersion       string
	)

	if migration.Direction == m.Up {
		migrationStatements = migration.Up
		insertVersion = "INSERT INTO " + postgresTableName + " (version) VALUES ($1)"
	} else if migration.Direction == m.Down {
		migrationStatements = migration.Down
		insertVersion = "DELETE FROM " + postgresTableName + " WHERE version=$1"
	}

	if migrationStatements.UseTransaction {
		tx, err := driver.db.Begin(ctx)
		if err != nil {
			return err
		}

		defer func() {
			if err != nil {
				if errRb := tx.Rollback(context.Background()); errRb != nil {
					err = fmt.Errorf("error rolling back: %s\n%s", errRb, err)
				}
				return
			}
			err = tx.Commit(ctx)
		}()

		for _, statement := range migrationStatements.Statements {
			if _, err = tx.Exec(ctx, statement); err != nil {
				return fmt.Errorf("error executing statement: %s\n%s", err, statement)
			}
		}

		if _, err = tx.Exec(ctx, insertVersion, migration.ID); err != nil {
			return fmt.Errorf("error updating migration versions: %s", err)
		}
	} else {
		for _, statement := range migrationStatements.Statements {
			if _, err := driver.db.Exec(ctx, statement); err != nil {
				return fmt.Errorf("error executing statement: %s\n%s", err, statement)
			}
		}
		if _, err = driver.db.Exec(ctx, insertVersion, migration.ID); err != nil {
			return fmt.Errorf("error updating migration versions: %s", err)
		}
	}
	return
}

// Versions lists all the applied versions.
func (driver *Driver) Versions(ctx context.Context) ([]string, error) {
	var versions []string

	rows, err := driver.db.Query(ctx, "SELECT version FROM "+postgresTableName+" ORDER BY version DESC")
	if err != nil {
		return versions, err
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		err = rows.Scan(&version)
		if err != nil {
			return versions, err
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return versions, nil
}
