package migration

import (
	"context"
	"errors"
	"strings"

	"github.com/muxinc/migration/parser"
)

type mockDriver struct {
	applied []string
}

func (m *mockDriver) Close(ctx context.Context) error {
	return nil
}

func (m *mockDriver) Migrate(ctx context.Context, migration *PlannedMigration) error {
	var migrationStatements *parser.ParsedMigration

	if migration.Direction == Up {
		migrationStatements = migration.Up
	} else {
		migrationStatements = migration.Down
	}

	errStatement := ""

	if len(migrationStatements.Statements) > 0 {
		errStatement = migrationStatements.Statements[0]
	}

	if strings.Contains(errStatement, "error") {
		return errors.New("error executing migration")
	}

	versionIndex := -1

	for i, version := range m.applied {
		if version == migration.ID {
			versionIndex = i
			break
		}
	}

	if migration.Direction == Up {
		if versionIndex == -1 {
			m.applied = append(m.applied, migration.ID)
		}
	} else {
		if versionIndex != -1 {
			m.applied = append(m.applied[:versionIndex], m.applied[versionIndex+1:]...)
		}
	}

	return nil
}

func (m *mockDriver) Versions(ctx context.Context) ([]string, error) {
	return m.applied, nil
}

func getMockDriver() *mockDriver {
	return &mockDriver{
		applied: []string{},
	}
}
