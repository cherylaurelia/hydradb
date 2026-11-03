#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python3}"

# This script calls cargo directly, so it never inherits the justfile's exports.
# graph-node's async query futures exceed the default thread stack: without this
# the node builds, serves /readyz, and then aborts with a stack overflow on the
# first query. Matches justfile:18. An already-exported value wins.
export RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}"

cargo build --locked --features server-runtime --bin graph-node

ROOT="${GRAPH_RUNTIME_SMOKE_ROOT:-/tmp/sgk-runtime-smoke}"
if [[ "$ROOT" != /tmp/sgk-runtime-smoke ]]; then
  echo "GRAPH_RUNTIME_SMOKE_ROOT must be /tmp/sgk-runtime-smoke" >&2
  exit 2
fi
rm -rf -- "$ROOT"
mkdir -p "$ROOT/store" "$ROOT/node-data-cache"

TOKEN="runtime-smoke-auth-token-32-characters-long"
printf '%s\n' "$TOKEN" >"$ROOT/auth-token"

export CLOUD_PROVIDER=local
export LOCAL_PATH="$ROOT/store"
export GRAPH_NAMESPACE=smoke
export GRAPH_ID=default
export GRAPH_CELL_ID=cell-0
export GRAPH_CELLS=cell-0
export GRAPH_DATA_PATH=data
export GRAPH_ALLOW_PLAINTEXT=true
export GRAPH_AUTH_TOKEN_FILE="$ROOT/auth-token"
export GRAPH_DATA_CACHE_BYTES=67108864
export GRAPH_BOLT_NODE_ADDRESSES=node-0=127.0.0.1:17687
export GRAPH_NODE_ID=node-0
export GRAPH_BOLT_ADDR=127.0.0.1:17687
export GRAPH_HTTP_ADDR=127.0.0.1:18443
export GRAPH_ADMIN_ADDR=127.0.0.1:19091
export GRAPH_ADVERTISED_BOLT_ADDR=127.0.0.1:17687
export GRAPH_DATA_CACHE_DIR="$ROOT/node-data-cache"
export RUST_LOG=warn

node_pid=""
cleanup() {
  if [[ -n "${node_pid:-}" ]]; then
    kill -TERM "$node_pid" 2>/dev/null || true
    wait "$node_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

target/debug/graph-node >"$ROOT/node.log" 2>&1 &
node_pid=$!
for _ in $(seq 1 160); do
  if curl -fsS http://127.0.0.1:19091/readyz >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$node_pid" 2>/dev/null; then
    cat "$ROOT/node.log" >&2
    exit 1
  fi
  sleep 0.25
done
curl -fsS http://127.0.0.1:19091/readyz >/dev/null
# Fetch first, then search: under pipefail, `curl | grep -q` fails with curl's
# exit 23 whenever grep matches and exits before a large /metrics body is
# fully written.
curl -fsS http://127.0.0.1:19091/metrics >"$ROOT/metrics.prom"
grep -q graph_runtime_ready "$ROOT/metrics.prom"

if ! "$PYTHON" -c 'import neo4j' >/dev/null 2>&1; then
  echo "python neo4j package is required for runtime smoke" >&2
  exit 2
fi
GRAPH_RUNTIME_SMOKE_TOKEN="$TOKEN" "$PYTHON" - <<'PY'
import os
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

with GraphDatabase.driver(
    "bolt://127.0.0.1:17687",
    auth=("neo4j", os.environ["GRAPH_RUNTIME_SMOKE_TOKEN"]),
) as driver:
    driver.verify_connectivity()
    with driver.session(database="default") as session:
        session.run("CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})").consume()
        row = session.run(
            "MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id"
        ).single(strict=True)
        assert row["id"] == 2, row

    scoped = {
        "default.scope1.dGVuYW50LWE.Y29sbGVjdGlvbi1h": 3,
        "default.scope1.dGVuYW50LWE.Y29sbGVjdGlvbi1i": 4,
    }
    for database, destination in scoped.items():
        with driver.session(database=database) as session:
            session.run(
                "CREATE (a {id: 1})-[:FOLLOWS]->(b {id: $destination})",
                destination=destination,
            ).consume()
    for database, destination in scoped.items():
        with driver.session(database=database) as session:
            rows = list(session.run(
                "MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id"
            ))
            assert [row["id"] for row in rows] == [destination], (database, rows)

    # execute_query and transaction functions send BEGIN, RUN, PULL, COMMIT.
    write = "MERGE (a {id: $src})-[:TX]->(b {id: $dst})"
    read = "MATCH (a {id: $src})-[:TX]->(b) RETURN b.id AS id"
    with driver.session(database="default") as session:
        def edges(src):
            return [row["id"] for row in session.run(read, src=src)]

        driver.execute_query(write, src=10, dst=11, database_="default")
        assert edges(10) == [11]
        records = driver.execute_query(read, src=10, database_="default").records
        assert [row["id"] for row in records] == [11], records
        session.execute_write(lambda tx: tx.run(write, src=20, dst=21).consume())
        assert session.execute_read(lambda tx: tx.run(read, src=20).data()) == [{"id": 21}]

        tx = session.begin_transaction()
        tx.run(write, src=30, dst=31).consume()
        tx.rollback()
        assert edges(30) == []

        def two_statements(tx):
            tx.run(write, src=40, dst=41).consume()
            tx.run(write, src=40, dst=42).consume()

        try:
            session.execute_write(two_statements)
        except ClientError as error:
            assert "one statement" in error.message, error
        else:
            raise AssertionError("a second statement must be rejected")
        assert edges(40) == []

        def read_then_write(tx):
            tx.run(read, src=10).consume()
            tx.run(write, src=50, dst=51).consume()

        def stream_then_run(tx):
            first = tx.run(read, src=60)
            next(iter(first))
            tx.run(read, src=10)

        def write_in_read(tx):
            tx.run(write, src=70, dst=71).consume()

        def refused(run, text):
            try:
                run()
            except ClientError as error:
                assert text in error.message, error
            else:
                raise AssertionError(f"expected a refusal: {text}")

        refused(lambda: session.execute_write(read_then_write), "one statement")
        assert edges(50) == []
        refused(lambda: session.execute_read(write_in_read), "read transaction")
        assert edges(70) == []
        session.run("MERGE (a:TX {id: 80})-[:TX]->(b {id: 81})").consume()
        refused(
            lambda: driver.execute_query(
                "MATCH (a:TX {id: 80}) SET a.x = 1 RETURN count(a) AS n", database_="default"
            ),
            "cannot return rows",
        )

        rows = [{"src": 60, "dst": 1000 + i} for i in range(20)]
        session.run(
            "UNWIND $rows AS row CREATE (a {id: row.src})-[:TX]->(b {id: row.dst})", rows=rows
        ).consume()
        with driver.session(database="default", fetch_size=5) as paged:
            refused(lambda: paged.execute_read(stream_then_run), "one statement")

        with driver.session(database="default") as bookmarked:
            bookmarked.execute_write(lambda tx: tx.run(write, src=90, dst=91).consume())
            assert bookmarked.last_bookmarks().raw_values

    scoped = "default.scope1.dGVuYW50LWE.Y29sbGVjdGlvbi1h"
    driver.execute_query(write, src=95, dst=96, database_=scoped, bookmark_manager_=None)
    with driver.session(database=scoped) as session:
        assert session.run(read, src=95).single(strict=True)["id"] == 96
    with driver.session(database="default") as session:
        assert session.run(read, src=95).single() is None
PY

http_result="$(curl -fsS -X POST \
  http://127.0.0.1:18443/v1/graphs/default/query \
  -H "Authorization: Bearer $TOKEN" \
  -H 'X-Graph-Namespace: smoke' \
  -H 'Content-Type: application/json' \
  --data '{"cell_id":"cell-0","query":"MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id"}')"
grep -q '"value":2' <<<"$http_result"

kill -TERM "$node_pid"
wait "$node_pid"
node_pid=""
echo runtime-smoke-ok
