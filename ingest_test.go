package postgres_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/nicovogelaar/postgres-test"
)

func TestIngestWeatherData(t *testing.T) {
	db := postgres.NewDB(postgres.DefaultURL)

	check(postgres.SetSynchronousCommit(db, false))
	check(postgres.SetCommitDelay(db, 20*time.Second))

	batchSize := 50000

	err := postgres.IngestWeatherData(context.Background(), db, batchSize, progressCallback)
	if err != nil {
		t.Fatalf("Failed to ingest weather data: %v", err)
	}
}

func progressCallback(filename string, i int, start time.Time) {
	s := time.Since(start).Seconds()
	log.Printf("Progress %v: %d / %.2f seconds / %d per second", filename, i, s, int(float64(i)/s))
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
