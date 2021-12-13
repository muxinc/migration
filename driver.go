package migration

import "context"

// Driver is the interface type that needs to implemented by all drivers.
type Driver interface {
	// Close is the last function to be called.
	//
	// Drivers should use this call to close any created connections or clean up
	// other resources as appropriate.
	Close(ctx context.Context) error

	// Migrate is the heart of the driver.
	// It will receive a PlannedMigration which the driver should apply
	// to its backend or whatever.
	//
	// Context can be used to cancel any incomplete migrations.
	Migrate(ctx context.Context, migration *PlannedMigration) error

	// Version returns all applied migration versions
	Versions(ctx context.Context) ([]string, error)
}
