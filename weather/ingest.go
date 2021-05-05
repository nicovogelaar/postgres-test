package weather

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/semaphore"
)

const (
	txSize         = 10000
	maxConcurrency = 5
)

type progressCallback func(filename string, i int, start time.Time, percent float64)

var sem = semaphore.NewWeighted(maxConcurrency)

func IngestWeatherData(ctx context.Context, db *sqlx.DB, batchSize int, cb progressCallback) error {
	if err := IngestLocations(ctx, db, locationsFilename, batchSize, cb); err != nil {
		return fmt.Errorf("ingest locations: %w", err)
	}

	if err := IngestConditions(ctx, db, conditionsFilename, batchSize, cb); err != nil {
		return fmt.Errorf("ingest conditions: %w", err)
	}

	return nil
}

func calcPercentage(bytesRead, size int64) float64 {
	return float64(bytesRead) / float64(size) * 100
}
