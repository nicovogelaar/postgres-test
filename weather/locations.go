package weather

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const locationsFilename = "weather_big_locations.csv"

func IngestLocations(ctx context.Context, db *sqlx.DB, filename string, batchSize int, cb progressCallback) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open file %v: %w", filename, err)
	}

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("file stat %v: %w", filename, err)
	}

	fileSize := fi.Size()

	c := newCountReader(file)

	r := csv.NewReader(c)

	var i int

	start := time.Now()

	batch := make([]location, 0, batchSize)

	var wg sync.WaitGroup

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if len(record) != 3 {
			continue
		}

		batch = append(batch, location{
			DeviceID:    record[0],
			Location:    record[1],
			Environment: record[2],
		})

		i++

		if i%batchSize == 0 {
			if err := sem.Acquire(ctx, 1); err != nil {
				return fmt.Errorf("semaphore acquire: %w", err)
			}

			log.Printf("c.bytesRead = %v, fileSize = %v", c.bytesRead, fileSize)

			wg.Add(1)
			go func(batch []location, i int, bytesRead int64) {
				defer sem.Release(1)
				defer wg.Done()

				if err = insertLocations(ctx, db, batch); err != nil {
					log.Printf("Failed to insert conditions: %v", err)
					return
				}

				cb(filename, i, start, calcPercentage(bytesRead, fileSize))
			}(batch, i, c.bytesRead)

			batch = make([]location, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		log.Printf("c.bytesRead = %v, fileSize = %v", c.bytesRead, fileSize)

		if err = insertLocations(ctx, db, batch); err != nil {
			return fmt.Errorf("insert locations: %w", err)
		}

		cb(filename, i, start, calcPercentage(c.bytesRead, fileSize))

		batch = nil
	}

	wg.Wait()

	return nil
}

func insertLocations(ctx context.Context, db *sqlx.DB, items []location) error {
	l := len(items)
	if l == 0 {
		return nil
	}

	var (
		previous int
		stmt     *sql.Stmt
	)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	for i, j := 0, txSize; i < l; i += txSize {
		if i+j > l {
			j = l - i
		}

		l2 := i + j

		args := make([]interface{}, 0, j*4)
		for i := i; i < l2; i++ {
			args = append(
				args,
				items[i].DeviceID,
				items[i].Location,
				items[i].Environment,
			)
		}

		if previous != j {
			query := "INSERT INTO locations (device_id, location, environment) VALUES ($1, $2, $3)"
			for i := 3; i < l*3; i += 3 {
				query += fmt.Sprintf(", ($%d, $%d, $%d)", i+1, i+2, i+3)
			}
			stmt, err = tx.PrepareContext(ctx, query)
			if err != nil {
				return fmt.Errorf("prepare: %w", err)
			}
			previous = j
		}

		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("exec: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
