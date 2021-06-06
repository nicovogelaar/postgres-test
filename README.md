# postgres-test

## weather

### Usage
```
$ cd weather
```

Download the csv files with the weather conditions and locations
```
$ ./download.sh
```

Run a docker postgres container
```
$ ./db.sh
postgres-test-db
60c3bd1c2a44400b45ead13695957576c71bc93dd13ce90591c1f9ca30867793
/var/run/postgresql:5432 - no response
/var/run/postgresql:5432 - rejecting connections
/var/run/postgresql:5432 - accepting connections
```

Run the test to ingest the data into postgres

Output format: `Progress [file name]: [number of rows processed] / [elapsed time] / [number of rows per second] / [percentage]`

```
$ go test -v .
=== RUN   TestIngestWeatherData
2021/06/06 09:13:44 c.bytesRead = 79209, fileSize = 79209
2021/06/06 09:13:44 Progress weather_big_locations.csv: 2000 / 0.08 seconds / 23644 per second / 100.00%
2021/06/06 09:13:50 Progress weather_big_conditions.csv: 100000 / 5.33 seconds / 18755 per second / 0.24%
2021/06/06 09:13:50 Progress weather_big_conditions.csv: 150000 / 5.70 seconds / 26314 per second / 0.37%
2021/06/06 09:13:51 Progress weather_big_conditions.csv: 50000 / 6.07 seconds / 8232 per second / 0.12%
2021/06/06 09:13:51 Progress weather_big_conditions.csv: 200000 / 6.28 seconds / 31843 per second / 0.49%
2021/06/06 09:13:51 Progress weather_big_conditions.csv: 250000 / 6.41 seconds / 39019 per second / 0.62%
2021/06/06 09:13:55 Progress weather_big_conditions.csv: 300000 / 10.07 seconds / 29784 per second / 0.75%
2021/06/06 09:13:55 Progress weather_big_conditions.csv: 350000 / 10.66 seconds / 32820 per second / 0.87%
2021/06/06 09:13:56 Progress weather_big_conditions.csv: 450000 / 11.07 seconds / 40640 per second / 1.12%
2021/06/06 09:13:56 Progress weather_big_conditions.csv: 500000 / 11.14 seconds / 44879 per second / 1.24%
2021/06/06 09:13:56 Progress weather_big_conditions.csv: 400000 / 11.55 seconds / 34645 per second / 0.99%
2021/06/06 09:13:59 Progress weather_big_conditions.csv: 550000 / 14.74 seconds / 37311 per second / 1.36%
2021/06/06 09:14:00 Progress weather_big_conditions.csv: 600000 / 15.79 seconds / 37988 per second / 1.48%
2021/06/06 09:14:01 Progress weather_big_conditions.csv: 650000 / 16.18 seconds / 40175 per second / 1.60%
2021/06/06 09:14:01 Progress weather_big_conditions.csv: 750000 / 16.33 seconds / 45930 per second / 1.84%
2021/06/06 09:14:01 Progress weather_big_conditions.csv: 700000 / 16.95 seconds / 41297 per second / 1.72%
2021/06/06 09:14:04 Progress weather_big_conditions.csv: 800000 / 19.93 seconds / 40150 per second / 1.96%
2021/06/06 09:14:06 Progress weather_big_conditions.csv: 900000 / 21.07 seconds / 42708 per second / 2.22%
2021/06/06 09:14:06 Progress weather_big_conditions.csv: 850000 / 21.17 seconds / 40159 per second / 2.09%
2021/06/06 09:14:06 Progress weather_big_conditions.csv: 950000 / 21.87 seconds / 43442 per second / 2.35%
2021/06/06 09:14:07 Progress weather_big_conditions.csv: 1000000 / 22.10 seconds / 45245 per second / 2.48%
2021/06/06 09:14:10 Progress weather_big_conditions.csv: 1050000 / 25.48 seconds / 41210 per second / 2.61%
2021/06/06 09:14:11 Progress weather_big_conditions.csv: 1150000 / 26.48 seconds / 43421 per second / 2.87%
2021/06/06 09:14:11 Progress weather_big_conditions.csv: 1100000 / 26.98 seconds / 40777 per second / 2.74%
2021/06/06 09:14:12 Progress weather_big_conditions.csv: 1200000 / 27.73 seconds / 43271 per second / 3.00%
2021/06/06 09:14:13 Progress weather_big_conditions.csv: 1250000 / 28.38 seconds / 44047 per second / 3.13%
2021/06/06 09:14:15 Progress weather_big_conditions.csv: 1300000 / 30.83 seconds / 42168 per second / 3.26%
...
```
