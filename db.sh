docker rm -f postgres-test-db
docker run -d --rm \
  -e POSTGRES_PASSWORD=postgres \
  --name postgres-test-db \
  -p 5432:5432 \
  -v "$(pwd)/db.sql:/docker-entrypoint-initdb.d/db.sql" \
  postgres:12
docker exec -it postgres-test-db bash -c 'while ! pg_isready; do sleep 1; done;'
