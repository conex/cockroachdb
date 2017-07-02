package cockroachdb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omeid/conex"
	// The driver.
	_ "github.com/lib/pq"
)

var (
	// Image to use for the box.
	Image = "cockroachdb/cockroach:v1.0.2"
	// Port used for connect to postgres server.
	Port = "26257"

	// PostgresUpWaitTime dectiates how long we should wait for post Postgresql to accept connections on {{Port}}.
	PostgresUpWaitTime = 10 * time.Second
)

func init() {
	conex.Require(func() string { return Image })
}

// Config used to connect to the database.
type Config struct {
	User     string
	Password string
	Database string // defaults to `postgres` as service db.

	host string
	port string
}

func (c *Config) url() string {
	// return an unauthenticated connection
	if c.Password == "" {
		fmt.Sprintf(
			"postgres://%s@%s:%s/%s?sslmode=disable",
			c.User, c.host, c.port, c.Database,
		)
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.User, c.Password, c.host, c.port, c.Database,
	)
}

func (c *Config) urlWithoutDatabase() string {
	// return an unauthenticated connection
	if c.Password == "" {
		fmt.Sprintf(
			"postgres://%s@%s:%s?sslmode=disable",
			c.User, c.host, c.port,
		)
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s?sslmode=disable",
		c.User, c.Password, c.host, c.port,
	)
}

// Box returns an sql.DB connection and the container running the Postgresql
// instance. It will call t.Fatal on errors.
func Box(t testing.TB, config *Config) (*sql.DB, conex.Container) {
	c := conex.Box(t, &conex.Config{
		Image:  Image,
		Cmd: []string{ "--insecure" },
		Expose: []string{ Port },
	})

	config.host = c.Address()
	config.port = Port

	t.Logf("Waiting for CockroachDB to accept connections")

	err := c.Wait(Port, PostgresUpWaitTime)

	if err != nil {
		c.Drop() // return the container
		t.Fatal("CockroachDB failed to start.", err)
	}

	t.Log("CockroachDB is now accepting connections")

	t.Log("Creating database if it does not exist")
	setupDB, err := sql.Open("postgres", config.urlWithoutDatabase())

	if err != nil {
		c.Drop() // return the container
		t.Fatal(err)
	}

	// create the database if it does not already exist, ignoring the error raised if it already exists
	_, _ = setupDB.Exec("CREATE database $1", config.Database)

	db, err := sql.Open("postgres", config.url())

	if err != nil {
		c.Drop() // return the container
		t.Fatal(err)
	}

	return db, c
}