#!/usr/bin/env bash
# Rate-limit enforcement accuracy under a 10x burst (blueprint §9).
#
# The demo key is provisioned burst=5, refill=1/s. A token bucket should
# admit at most  burst + elapsed*refill  requests, no matter how hard it
# is hammered. Accuracy is measured against that prediction, not a guess.
#
# Run from the repo root:  bash benchmarks/ratelimit_test.sh
set -uo pipefail

KEY="${KEY:-sk-forge-demo-key}"
URL="${URL:-http://localhost:8080}"
BURST="${BURST:-5}"
REFILL="${REFILL:-1}"
N="${N:-50}"          # 10x burst
OUT="benchmarks/raw/ratelimit.csv"
mkdir -p benchmarks/raw
echo "i,status,elapsed_ms" > "$OUT"

start=$(date +%s%3N)
allowed=0; limited=0; other=0
for i in $(seq 1 "$N"); do
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/v1/chat/completions" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $KEY" \
    -d '{"max_tokens":4,"messages":[{"role":"user","content":"hi"}]}')
  now=$(date +%s%3N)
  echo "$i,$code,$((now - start))" >> "$OUT"
  case "$code" in
    200) allowed=$((allowed+1));;
    429) limited=$((limited+1));;
    *)   other=$((other+1));;
  esac
done
end=$(date +%s%3N)

python - "$OUT" "$BURST" "$REFILL" "$allowed" "$limited" "$other" "$((end-start))" <<'PY'
import sys
_, out, burst, refill, allowed, limited, other, elapsed_ms = sys.argv
burst, refill = float(burst), float(refill)
allowed, limited, other = int(allowed), int(limited), int(other)
elapsed = int(elapsed_ms) / 1000.0

# A correct bucket admits at most burst + elapsed*refill over the window.
budget = burst + elapsed * refill
leak = max(0.0, allowed - budget)
accuracy = 100.0 * (1 - leak / budget) if budget else 0.0

print(f"window: {elapsed:.2f}s   burst={burst:.0f}  refill={refill:.0f}/s")
print(f"requests: {allowed + limited + other}  allowed={allowed}  "
      f"429={limited}  other={other}")
print(f"token budget for this window: {budget:.2f}")
print(f"over-admitted (leak): {leak:.2f}")
print(f"enforcement accuracy: {accuracy:.2f}%")
ok = other == 0 and accuracy > 99.0
print("RATE LIMIT TEST:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
