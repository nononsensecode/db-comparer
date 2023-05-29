package dbcomparer_test

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/tern/migrate"
	comparer "github.com/nononsensecode/db-comparer"
)

func (s *IntegrationTestSuite) TestCompare() {
	tests := map[string]struct {
		datasetFile    string
		migrateVersion int32
		matched        bool
		wantedErr      error
	}{
		"compare with matched dataset": {
			datasetFile:    "test/datasets/employee.yml",
			migrateVersion: 2,
			matched:        true,
			wantedErr:      nil,
		},
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.PgDbURL)
	s.Require().NoError(err)
	defer conn.Close(ctx)

	m, err := migrate.NewMigrator(ctx, conn, "schema_version")
	s.Require().NoError(err)

	err = m.LoadMigrations("test/db/migrations")
	s.Require().NoError(err)

	for name, tc := range tests {
		s.Run(name, func() {
			err = m.MigrateTo(ctx, tc.migrateVersion)
			s.Require().NoError(err)

			c := comparer.New(func(ctx context.Context, connStr string) (comparer.PgxIface, error) {
				return pgxpool.Connect(ctx, connStr)
			}, s.PgDbURL)

			// Compare the dataset
			matched, err := c.Compare(tc.datasetFile, nil, nil)
			s.Require().Equal(tc.wantedErr, err)
			s.Require().Equal(tc.matched, matched)
		})
	}

	err = m.MigrateTo(ctx, 0)
	s.Require().NoError(err)
}
