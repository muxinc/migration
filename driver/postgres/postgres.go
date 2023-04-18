package postgres

import (
	"context"
	"fmt"

	m "github.com/GRVYDEV/migration"
	"github.com/GRVYDEV/migration/parser"
	"github.com/jackc/pgx/v5"
)

// Driver is the postgres migration.Driver implementation
type Driver struct {
	conn *pgx.Conn
	// closeConnOnClose indicates whether or not conn should be closed upon
	// Driver.Close(). It is set to true if the conn was created by the Driver
	// rather than passed in.
	closeConnOnClose bool
}

const postgresTableName = "schema_migration"

// New creates a new Driver and initializes a connection to the database. The
// context can be used to cancel the connection attempt.
//
// The DSN is documented here: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib#pkg-overview
//
// If a conn has been created, it will be closed when Close() is called on the
// returned Driver.
func New(ctx context.Context, dsn string) (m.Driver, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	d, err := newFromConn(ctx, conn)
	if err != nil {
		conn.Close(ctx)
		return nil, err
	}
	// ensure that this conn is closed upon Driver.Close():
	d.closeConnOnClose = true
	return d, err
}

// NewFromConn creates a new Driver from an existing database connection. The
// connection is pinged for availability before returning, and ctx can be used
// to cancel the ping attempt.
//
// The conn will be closed after migrations complete (when Close() is called on
// the driver).
func NewFromConn(ctx context.Context, conn *pgx.Conn) (m.Driver, error) {
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return newFromConn(ctx, conn)
}

func newFromConn(ctx context.Context, conn *pgx.Conn) (*Driver, error) {
	d := &Driver{
		conn: conn,
	}
	if err := d.ensureVersionTableExists(ctx); err != nil {
		return nil, err
	}

	return d, nil
}

// Close closes the connection to the Driver server.
func (driver *Driver) Close(ctx context.Context) error {
	if driver.closeConnOnClose {
		return driver.conn.Close(ctx)
	}
	return nil
}

func (driver *Driver) ensureVersionTableExists(ctx context.Context) error {
	_, err := driver.conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+postgresTableName+" (version varchar(255) not null primary key)")
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
		tx, err := driver.conn.Begin(ctx)
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
			if _, err := driver.conn.Exec(ctx, statement); err != nil {
				return fmt.Errorf("error executing statement: %s\n%s", err, statement)
			}
		}
		if _, err = driver.conn.Exec(ctx, insertVersion, migration.ID); err != nil {
			return fmt.Errorf("error updating migration versions: %s", err)
		}
	}
	return
}

// Versions lists all the applied versions.
func (driver *Driver) Versions(ctx context.Context) ([]string, error) {
	var versions []string

	rows, err := driver.conn.Query(ctx, "SELECT version FROM "+postgresTableName+" ORDER BY version DESC")
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
