import math
import pytest
from typing import List

from stream.hub import TickerHub
from stream.coalescer import TickerTick as Tick
from features.microstructure import MicroFeatureEngine
from features.indicators import IndiEngine


def make_ticks(symbol: str, n: int) -> List[Tick]:
    """
    Генерим n тиков с лёгким трендом и узким спредом.
    ts растёт по 100мс, bid/ask сдвигаются, sizes > 0.
    """
    base = 111_000.0
    ticks: List[Tick] = []
    for i in range(n):
        ts = 1_700_000_000_000 + i * 100  # монотонный ts_ms
        mid = base + i * 0.5 + (i % 5) * 0.1
        bid = mid - 0.05
        ask = mid + 0.05
        price = mid  # используем mid как price
        bid_sz = 3.0 + (i % 7) * 0.2
        ask_sz = 4.0 + (i % 5) * 0.3
        mark = mid + 0.01
        ticks.append(
            Tick(
                ts_ms=ts,
                symbol=symbol,
                price=price,
                bid=bid,
                ask=ask,
                bid_size=bid_sz,
                ask_size=ask_sz,
                mark_price=mark,
            )
        )
    return ticks


@pytest.mark.asyncio
async def test_hub_coalescer_and_features_ok():
    symbol = "BTCUSDT"
    N = 120  # достаточно, чтобы индикаторы накопили окна

    # --- хаб (он внутри себя умеет учитывать "коалесцированные" тики)
    hub = TickerHub(symbols=[symbol], coalesce_ms=75)

    data = make_ticks(symbol, N)

    # --- вместо явного Coalescer "кормим" хаб его приватным приёмником тиков
    # делаем это намеренно, чтобы тест не зависел от сигнатуры Coalescer.__init__
    assert hasattr(hub, "_on_tick"), "TickerHub должен иметь _on_tick(tick: TickerTick)"
    for t in data:
        # приватный метод асинхронный — вызываем с await
        await hub._on_tick(t)  # type: ignore[attr-defined]

    # --- проверяем счётчики коалесцера через публичный API хаба
    coal_stats = hub.coalescer_stats()
    assert isinstance(coal_stats, dict)
    assert coal_stats.get("push_count") == N
    assert coal_stats.get("last_tick_ts_ms") == data[-1].ts_ms

    # --- ws_stats могут быть нулевыми (мы не гоняли WS), проверяем, что форма корректная
    ws_stats = hub.ws_stats()
    assert isinstance(ws_stats, dict)
    assert "count" in ws_stats and "last_rx_ts_ms" in ws_stats

    # --- забираем последний тик и считаем фичи
    last = hub.latest(symbol)
    assert last is not None, "hub.latest() должен вернуть последний тик"

    micro = MicroFeatureEngine(tick_size=0.1, lot_usd=10_000.0)
    indi = IndiEngine(price_step=0.1)

    # micro — по последнему снапу
    mf = micro.update(
        price=last.price,
        bid=last.bid,
        ask=last.ask,
        bid_sz=last.bid_size,
        ask_sz=last.ask_size,
    )

    # индикаторы — по всей истории
    for t in data:
        ind = indi.update(price=t.price)

    # --- базовые проверки микроструктуры
    assert mf.spread_ticks >= 0
    assert mf.top_liq_usd > 0
    assert -1.0 <= mf.obi <= 1.0
    # tick_velocity: допускаем None (на старте) или число без NaN/Inf
    assert (mf.tick_velocity is None) or (
        isinstance(mf.tick_velocity, (int, float))
        and not math.isnan(mf.tick_velocity)
        and not math.isinf(mf.tick_velocity)
    )

    # --- базовые проверки индикаторов
    assert isinstance(ind.ema9, float) and isinstance(ind.ema21, float)
    assert not math.isnan(ind.ema9) and not math.isnan(ind.ema21)
    assert not math.isinf(ind.ema9) and not math.isinf(ind.ema21)

    assert ind.realized_vol_bp >= 0
    assert not math.isnan(ind.realized_vol_bp)
    assert not math.isinf(ind.realized_vol_bp)

    assert 0.0 <= ind.rsi <= 100.0
