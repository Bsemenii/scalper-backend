#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
SYMBOL="${SYMBOL:-BTCUSDT}"

ok() { printf "✅ %s\n" "$1"; }
warn() { printf "⚠️  %s\n" "$1"; }
err() { printf "❌ %s\n" "$1"; }

jqget() { jq -r "$1" 2>/dev/null || true; }

echo "BASE=$BASE SYMBOL=$SYMBOL"

# 1) Базовые
root=$(curl -sS "$BASE/") && echo "$root" | jq . >/dev/null && ok "/ - up ($(echo "$root"|jqget .version))" || err "/ failed"
health=$(curl -sS "$BASE/healthz") && ok "/healthz ws=$(echo "$health"|jqget .ws_messages) emits=$(echo "$health"|jqget .emits)" || err "/healthz failed"
status=$(curl -sS "$BASE/status") && ok "/status mode=$(echo "$status"|jqget .mode) symbols=$(echo "$status"|jq -r .symbols|tr -d '\n')" || err "/status failed"

# 2) Конфиг
conf=$(curl -sS "$BASE/config/active") && \
  ok "/config/active min_stop_ticks=$(echo "$conf"|jqget .execution.min_stop_ticks)" || err "/config/active failed"

# 3) Диагностика
diag=$(curl -sS "$BASE/debug/diag") && ok "/debug/diag ws_errors=$(echo "$diag"|jqget .ws_detail.errors) coalesce_emit=$(echo "$diag"|jqget .coal.emit_count)" || err "/debug/diag failed"
metrics=$(curl -sS "$BASE/metrics") && ok "/metrics emits_per_sec=$(echo "$metrics"|jqget .coalesce_emits_per_sec)" || err "/metrics failed"
reasons=$(curl -sS "$BASE/debug/reasons") && ok "/debug/reasons $(echo "$reasons"|jq -c .reasons)" || err "/debug/reasons failed"

# 4) Поток и фичи
best=$(curl -sS "$BASE/best?symbol=$SYMBOL") && ok "/best bid=$(echo "$best"|jqget .bid) ask=$(echo "$best"|jqget .ask)" || err "/best failed"
latest=$(curl -sS "$BASE/ticks/latest?symbol=$SYMBOL") && ok "/ticks/latest ts=$(echo "$latest"|jqget .item.ts_ms)" || err "/ticks/latest failed"
peek=$(curl -sS "$BASE/ticks/peek?symbol=$SYMBOL&ms=1500&limit=64") && ok "/ticks/peek count=$(echo "$peek"|jqget .count)" || err "/ticks/peek failed"
feats=$(curl -sS "$BASE/debug/features?symbol=$SYMBOL&lookback_ms=10000") && ok "/debug/features allow=$(echo "$feats"|jqget .risk.allow)" || err "/debug/features failed"

# 5) Позиции/сделки
pos=$(curl -sS "$BASE/position?symbol=$SYMBOL") && ok "/position state=$(echo "$pos"|jqget .position.state)" || err "/position failed"
trades=$(curl -sS "$BASE/trades?symbol=$SYMBOL&limit=5") && ok "/trades count=$(echo "$trades"|jqget .count)" || err "/trades failed"

# 6) Smoke исполнение (paper)
entry=$(curl -sS "$BASE/control/test-entry?symbol=$SYMBOL&side=BUY&qty=0.001")
if echo "$entry" | jq -e '.ok==true' >/dev/null; then
  ok "test-entry placed (avg_px=$(echo "$entry"|jqget .report.avg_px))"
else
  warn "test-entry: $(echo "$entry"|jq -c .)"
fi

# Попытка автосайзинга (может вернуть busy:OPEN — это нормально)
auto=$(curl -sS "$BASE/control/test-entry-auto?symbol=$SYMBOL&side=SELL") && echo "$auto" | jq . >/dev/null && ok "test-entry-auto $(echo "$auto"|jq -c .)" || warn "test-entry-auto failed"

# Форс-закрытие
flat=$(curl -sS "$BASE/control/flatten?symbol=$SYMBOL") && ok "flatten $(echo "$flat"|jq -c .reason)" || err "flatten failed"

# Итоговые метрики/PNL
metrics2=$(curl -sS "$BASE/metrics") && ok "metrics post-trade orders_limit_total=$(echo "$metrics2"|jqget .orders_limit_total) trades_day=$(echo "$metrics2"|jqget .trades_day)" || warn "metrics post failed"
pnl=$(curl -sS "$BASE/pnl/daily") && ok "pnl/day pnl_usd=$(echo "$pnl"|jqget .pnl_usd) trades=$(echo "$pnl"|jqget .trades)" || warn "pnl/day failed"

echo "— done —"
