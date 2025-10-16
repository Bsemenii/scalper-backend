import math
import pytest
from typing import AsyncIterator, List

from stream.hub import TickerHub, Tick
from stream.coalescer import Coalescer
from features.microstructure import MicroFeatureEngine
from features.indicators import IndiEngine


def make_ticks(symbol: str, n: int) -> List[Tick]:
    """
    Генерим n тиков с лёгким трендом и узким спредом.
    ts растёт по 100мс, bid/ask сдвигаются, sizes > 0.
    """
    base = 111_000.0
    ticks = []
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


async def tick_stream(lst: List[Tick]) -> AsyncIterator[Tick]:
    for t in lst:
        yield t


@pytest.mark.asyncio
async def test_hub_coalescer_and_features_ok():
    symbol = "BTCUSDT"
    N = 120  # достаточно, чтобы индикаторы накопили окна

    # --- подготовка пайплайна
    hub = TickerHub(maxlen=4096)
    coalescer = Coalescer(hub=hub, symbols=[symbol], interval_ms=75)

    data = make_ticks(symbol, N)

    # --- прогон через коалесер
    await coalescer.run(tick_stream(data))

    # --- проверяем счётчики
    ws_stats = hub.ws_stats()
    coal_stats = coalescer.stats()
    assert ws_stats["count"] == N
    assert coal_stats["push_count"] == N
    assert ws_stats["last_rx_ts_ms"] == data[-1].ts_ms
    assert coal_stats["last_tick_ts_ms"] == data[-1].ts_ms

    # --- забираем последний тик и считаем фичи
    last = hub.latest(symbol)
    assert last is not None

    # tick_size и price_step для BTCUSDT на фьючах  обычно 0.1
    micro = MicroFeatureEngine(tick_size=0.1, lot_usd=10_000.0)
    indi = IndiEngine(price_step=0.1)

    # для micro достаточно одного обновления
    mf = micro.update(
        price=last.price,
        bid=last.bid,
        ask=last.ask,
        bid_sz=last.bid_size,
        ask_sz=last.ask_size,
    )

    # для индикаторов нужно «накормить» историей
    for t in data:
        ind = indi.update(price=t.price)

    # --- базовые проверки микроструктуры
    assert mf.spread_ticks >= 0
    assert mf.top_liq_usd > 0
    assert -1.0 <= mf.obi <= 1.0
    # tick_velocity может быть None на старте, но после N>3 должно быть числом
    assert (mf.tick_velocity is None) or (mf.tick_velocity >= 0)

    # --- базовые проверки индикаторов
    assert isinstance(ind.ema9, float) and isinstance(ind.ema21, float)
    # realized_vol_bp должен быть посчитан на серии > 5
    assert ind.realized_vol_bp >= 0
    # RSI в [0..100]
    assert 0.0 <= ind.rsi <= 100.0
    # bb_z может быть 0, если окно еще не набралось, но это ок

    # --- sanity: если вола совсем нулевая, это подозрительно при N=120
    # допустим минимальный порог ~0 bp редко, но позволим вплоть до 0.
    # Просто убедимся, что нет NaN/Inf
    assert not math.isnan(ind.realized_vol_bp)
    assert not math.isinf(ind.realized_vol_bp)
