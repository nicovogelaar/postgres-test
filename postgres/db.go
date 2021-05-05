package postgres

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const DefaultURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

func NewDB(url string) *sqlx.DB {
	return sqlx.MustConnect("postgres", url)
}

func SetSynchronousCommit(db *sqlx.DB, syncCommit bool) error {
	val := "on"
	if !syncCommit {
		val = "off"
	}
	_, err := db.Exec(fmt.Sprintf(`SET synchronous_commit TO %s;`, val))
	return err
}

func SetCommitDelay(db *sqlx.DB, val time.Duration) error {
	_, err := db.Exec(fmt.Sprintf(`SET commit_delay TO %d;`, val.Milliseconds()))
	return err
}
