#!/usr/bin/env bash
# Multi-container end-to-end test for A2A Events.
#
# Brings up Postgres + a publisher service + a subscriber service in temporary
# containers on a private Docker network, then runs the host-side driver that
# exercises every feature over the real network. Tears everything down on exit.
set -euo pipefail

NET=a2a-e2e-net
PG=a2a-e2e-pg
PUBC=a2a-e2e-pub
SUBC=a2a-e2e-sub
IMAGE=a2a-e2e
DB_URL="postgresql://postgres:pw@${PG}:5432/a2a"
# Repo root: this script lives at <repo>/e2e/. The Docker build context is the
# repo root so pyproject.toml's readme = "README.md" resolves during the build.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cleanup() {
	echo "--- cleanup ---"
	docker rm -f "$PUBC" "$SUBC" "$PG" >/dev/null 2>&1 || true
	docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup # clear any leftovers from a previous run

echo "--- build image ---"
docker build -t "$IMAGE" -f "$ROOT/e2e/Dockerfile" "$ROOT"

echo "--- network + postgres ---"
docker network create "$NET" >/dev/null
docker run -d --name "$PG" --network "$NET" \
	-e POSTGRES_PASSWORD=pw -e POSTGRES_DB=a2a postgres:16-alpine >/dev/null
for _ in $(seq 1 30); do
	docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break
	sleep 1
done

echo "--- publisher ---"
# The publisher discovers the subscriber's A2A endpoint from its AgentCard
# (§12.2, §21.2), so it only needs the DB, its key seed, and the receive URL
# used by the /admin/deliver-skewed scaffold.
docker run -d --name "$PUBC" --network "$NET" -p 18080:8000 \
	-e DATABASE_URL="$DB_URL" \
	-e SUBSCRIBER_RECEIVE_URL="http://${SUBC}:8000/a2a-events/receive" \
	-e PUBLISHER_KEY_SEED="$(printf '11%.0s' {1..32})" \
	"$IMAGE" >/dev/null

echo "--- subscriber ---"
docker run -d --name "$SUBC" --network "$NET" -p 18081:8000 \
	-e PUBLISHER_JWKS_URL="http://${PUBC}:8000/a2a-events/keys" \
	"$IMAGE" uvicorn e2e.subscriber_service:app --host 0.0.0.0 --port 8000 >/dev/null

echo "--- driver ---"
cd "$ROOT"
uv run python e2e/driver.py

echo "--- durability: restart publisher, subscription must persist (Postgres) ---"
docker restart "$PUBC" >/dev/null
for _ in $(seq 1 30); do
	curl -fs http://localhost:18080/admin/health >/dev/null 2>&1 && break
	sleep 1
done
COUNT=$(curl -fs http://localhost:18080/a2a-events/subscriptions |
	uv run python -c "import sys,json; print(len(json.load(sys.stdin)['subscriptions']))")
if [ "$COUNT" -ge 1 ]; then
	echo "  PASS  $COUNT subscription(s) survived publisher restart"
else
	echo "  FAIL  no subscriptions after restart"
	exit 1
fi

echo "ALL END-TO-END CHECKS PASSED"
