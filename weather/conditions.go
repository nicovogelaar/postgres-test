package weather

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const conditionsFilename = "weather_big_conditions.csv"

func IngestConditions(ctx context.Context, db *sqlx.DB, filename string, batchSize int, cb progressCallback) error {
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

	batch := make([]condition, 0, batchSize)

	var wg sync.WaitGroup

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if len(record) != 4 {
			continue
		}
		temperature, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			return fmt.Errorf("parse temperature: %w", err)
		}
		humidity, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			return fmt.Errorf("parse humidity: %w", err)
		}

		batch = append(batch, condition{
			Time:        time.Time{},
			DeviceID:    record[1],
			Temperature: temperature,
			Humidity:    humidity,
		})

		i++

		if i%batchSize == 0 {
			if err := sem.Acquire(ctx, 1); err != nil {
				return fmt.Errorf("semaphore acquire: %w", err)
			}

			wg.Add(1)
			go func(batch []condition, i int, bytesRead int64) {
				defer sem.Release(1)
				defer wg.Done()

				if err = insertConditions(ctx, db, batch); err != nil {
					log.Printf("Failed to insert conditions: %v", err)
					return
				}

				cb(filename, i, start, calcPercentage(bytesRead, fileSize))
			}(batch, i, c.bytesRead)

			batch = make([]condition, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		if err = insertConditions(ctx, db, batch); err != nil {
			return fmt.Errorf("insert conditions: %w", err)
		}

		cb(filename, i, start, calcPercentage(c.bytesRead, fileSize))

		batch = nil
	}

	wg.Wait()

	return nil
}

func insertConditions(ctx context.Context, db *sqlx.DB, items []condition) error {
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
				items[i].Time,
				items[i].DeviceID,
				items[i].Temperature,
				items[i].Humidity,
			)
		}

		if previous != j {
			query := "INSERT INTO conditions (time, device_id, temperature, humidity) VALUES ($1, $2, $3, $4)"
			for i := 4; i < j*4; i += 4 {
				query += fmt.Sprintf(", ($%d, $%d, $%d, $%d)", i+1, i+2, i+3, i+4)
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
