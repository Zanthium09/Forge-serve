#!/usr/bin/env bash
# Chaos test: kill a worker under sustained load and assert the fleet
# survives it (blueprint Phase 3 exit criterion).
#
# Asserts:
#   1. Zero accepted-request loss - every request ends with a real HTTP
#      status, never a dropped connection.
#   2. Recovery < 5s - a successful completion lands within 5s of the kill.
#
# Run from the repo root in Git Bash:  bash benchmarks/chaos_test.sh
set -uo pipefail

DURATION="${DURATION:-60}"
VICTIM="${VICTIM:-forge-serve-worker2-1}"
KEY="${KEY:-sk-forge-dev-key}"
URL="${URL:-http://localhost:8080}"
OUT="benchmarks/raw/chaos.csv"
mkdir -p benchmarks/raw
echo "unix_ms,status,ok" > "$OUT"

echo "=== chaos: ${DURATION}s of load, killing $VICTIM at the midpoint ==="

LOADPIDS=()
for i in $(seq 1 6); do
  (
    end=$(( $(date +%s) + DURATION ))
    while [ "$(date +%s)" -lt "$end" ]; do
      code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 120 \
        -X POST "$URL/v1/chat/completions" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $KEY" \
        -d '{"max_tokens":24,"messages":[{"role":"user","content":"hi"}]}' 2>/dev/null)
      # An empty code means the connection itself failed: that is request
      # loss, which is exactly what this test looks for.
      [ -z "$code" ] && code="000"
      ok=0
      [ "$code" = "200" ] && ok=1
      echo "$(date +%s%3N),$code,$ok" >> "$OUT"
    done
  ) &
  LOADPIDS+=($!)
done

sleep $(( DURATION / 2 ))
echo "--- killing $VICTIM ---"
KILL_MS=$(date +%s%3N)
docker kill "$VICTIM" >/dev/null 2>&1 || echo "could not kill $VICTIM"
echo "killed at ${KILL_MS}"

for pid in "${LOADPIDS[@]}"; do wait "$pid"; done

echo
echo "=== results ==="
python - "$OUT" "$KILL_MS" <<'PY'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
kill_ms = int(sys.argv[2])
lost = [r for r in rows if r["status"] == "000"]
ok = [r for r in rows if r["ok"] == "1"]
codes = {}
for r in rows:
    codes[r["status"]] = codes.get(r["status"], 0) + 1

after = sorted(int(r["unix_ms"]) for r in ok if int(r["unix_ms"]) > kill_ms)
recovery = (after[0] - kill_ms) / 1000.0 if after else None

print(f"requests: {len(rows)}   ok: {len(ok)}   dropped connections: {len(lost)}")
print(f"status codes: {codes}")
if recovery is None:
    print("no successful request after the kill")
else:
    print(f"first success after kill: {recovery:.2f}s")

fail = False
if lost:
    print(f"FAIL: {len(lost)} requests lost their connection")
    fail = True
if recovery is None or recovery > 5.0:
    print("FAIL: recovery exceeded 5s")
    fail = True
print("CHAOS TEST:", "FAIL" if fail else "PASS")
sys.exit(1 if fail else 0)
PY
