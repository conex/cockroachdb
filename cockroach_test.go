package cockroachdb_test

import (
	"os"
	"testing"

	"github.com/omeid/conex"
	"github.com/conex/cockroachdb"
)

func TestMain(m *testing.M) {
	os.Exit(conex.Run(m))
}

func TestPostgres(t *testing.T) {

	sql, con := cockroachdb.Box(t, &cockroachdb.Config{
		Database: "test",
		User:     "root",
	})
	defer con.Drop()

	var resp int
	err := sql.QueryRow("SELECT 1").Scan(&resp)

	if err != nil {
		t.Fatal(err)
	}

	if resp != 1 {
		t.Fatal("Unexpected response: %v")
	}

}