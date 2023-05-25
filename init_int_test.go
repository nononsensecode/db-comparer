package dbcomparer_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/tern/migrate"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/suite"
)

type IntegrationTestSuite struct {
	suite.Suite
	HostAndPort    string
	DockerPool     *dockertest.Pool
	DockerResource *dockertest.Resource
	PgUsername     string
	PgPassword     string
	PgDB           string
	PgDbURL        string
}

func (s *IntegrationTestSuite) SetupSuite() {
	s.PgUsername = "compuser"
	s.PgPassword = "comppasswd"
	s.PgDB = "comparer"

	var err error
	s.DockerPool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = s.DockerPool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	s.DockerResource, err = s.DockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "13-alpine",
		Env: []string{
			"POSTGRES_PASSWORD=" + s.PgPassword,
			"POSTGRES_USER=" + s.PgUsername,
			"POSTGRES_DB=" + s.PgDB,
			"listen_addresses = '*'",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	s.HostAndPort = s.DockerResource.GetHostPort("5432/tcp")
	s.PgDbURL = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		s.PgUsername, s.PgPassword, s.HostAndPort, s.PgDB)

	log.Println("Connecting to database on url: ", s.PgDbURL)

	s.DockerResource.Expire(120)

	s.DockerPool.MaxWait = 120 * time.Second
	if err = s.DockerPool.Retry(func() error {
		ctx := context.Background()
		db, err := pgx.Connect(ctx, s.PgDbURL)
		if err != nil {
			return err
		}
		return db.Ping(ctx)
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
}

func (s *IntegrationTestSuite) TearDownSuite() {
	if err := s.DockerPool.Purge(s.DockerResource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
}

func (s *IntegrationTestSuite) SetupTest() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.PgDbURL)
	s.Require().NoError(err)
	defer conn.Close(ctx)

	m, err := migrate.NewMigrator(ctx, conn, "schema_version")
	s.Require().NoError(err)

	err = m.LoadMigrations("test/db/migrations")
	s.Require().NoError(err)

	err = m.MigrateTo(ctx, 1)
	s.Require().NoError(err)
}

func (s *IntegrationTestSuite) TearDownTest() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.PgDbURL)
	s.Require().NoError(err)
	defer conn.Close(ctx)

	m, err := migrate.NewMigrator(ctx, conn, "schema_version")
	s.Require().NoError(err)

	err = m.LoadMigrations("test/db/migrations")
	s.Require().NoError(err)

	err = m.MigrateTo(ctx, 0)
	s.Require().NoError(err)
}

func TestComparerTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
