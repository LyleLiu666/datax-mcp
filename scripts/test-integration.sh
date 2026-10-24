#!/usr/bin/env bash
# Disposable fixtures only. Does not connect to configured business databases.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
export DATAX_HOME="${DATAX_HOME:-$root/.cache/datax}"
[[ -f "$DATAX_HOME/conf/core.json" ]] || { echo 'Build DataX with scripts/build-datax.sh first.' >&2; exit 1; }
suffix="$$-$(date +%s)"
pg_container="datax-mcp-pg-$suffix"
mysql_container="datax-mcp-mysql-$suffix"
cleanup() { docker rm -f "$pg_container" "$mysql_container" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM
docker run --rm -d --name "$pg_container" -p 127.0.0.1::5432 -e POSTGRES_PASSWORD=fixture-target -e POSTGRES_DB=datax_mcp_test postgres:17.11 >/dev/null
docker run --rm -d --name "$mysql_container" -p 127.0.0.1::3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.4.11 --mysql-native-password=ON >/dev/null
ready=false
for ((i=0;i<120;i++)); do
 if docker exec "$pg_container" pg_isready -U postgres >/dev/null 2>&1 && docker exec "$mysql_container" mysql -uroot -e 'SELECT 1' >/dev/null 2>&1; then ready=true;break;fi
 sleep 1
done
[[ "$ready" == true ]] || { echo 'Fixture databases did not start.' >&2; exit 1; }
docker exec -i "$mysql_container" mysql -uroot < scripts/mysql-fixture.sql
pg_port=$(docker port "$pg_container" 5432/tcp); pg_port=${pg_port##*:}
mysql_port=$(docker port "$mysql_container" 3306/tcp); mysql_port=${mysql_port##*:}
export DATAX_TEST_PG_DSN="postgresql://postgres:fixture-target@127.0.0.1:$pg_port/datax_mcp_test?sslmode=disable"
export DATAX_TEST_MYSQL_JDBC="jdbc:mysql://127.0.0.1:$mysql_port/sync_fixture?useSSL=false&useUnicode=true&characterEncoding=UTF-8"
go test -count=1 -race -v ./...
