package weather

import "time"

type location struct {
	DeviceID    string
	Location    string
	Environment string
}

type condition struct {
	Time        time.Time
	DeviceID    string
	Temperature float64
	Humidity    float64
}
