import os
import time
import asyncio
import ssl
from decimal import Decimal, ROUND_DOWN

from dotenv import load_dotenv
import aiohttp
import certifi

from adapters.binance_rest import BinanceUSDTMAdapter, OrderReq

# Дебаг: покажем, что модуль вообще загрузился
print(">>> tools.test_binance_testnet: module imported")

SSL_CTX = ssl.create_default_context(cafile=certifi.where())


async def test_server_time(base_url: str) -> bool:
    print("⏱ Тест /fapi/v1/time ...")
    connector = aiohttp.TCPConnector(ssl=SSL_CTX)

    async with aiohttp.ClientSession(connector=connector) as sess:
        async with sess.get(f"{base_url}/fapi/v1/time") as resp:
            txt = await resp.text()
            if resp.status != 200:
                print(f"❌ /time status={resp.status}, body={txt}")
                return False
            try:
                data = await resp.json()
            except Exception:
                print("❌ /time вернул не-JSON:", txt)
                return False

    print("✅ Сервер отвечает, serverTime:", data.get("serverTime"))
    return True


async def get_symbol_filters(base_url: str, symbol: str):
    print(f"🔍 Забираем exchangeInfo для {symbol} ...")
    connector = aiohttp.TCPConnector(ssl=SSL_CTX)

    async with aiohttp.ClientSession(connector=connector) as sess:
        async with sess.get(f"{base_url}/fapi/v1/exchangeInfo", params={"symbol": symbol}) as resp:
            txt = await resp.text()
            if resp.status != 200:
                print(f"❌ /exchangeInfo status={resp.status}, body={txt}")
                return None, None
            try:
                info = await resp.json()
            except Exception:
                print("❌ /exchangeInfo вернул не-JSON:", txt)
                return None, None

    try:
        s = info["symbols"][0]
        price_filter = next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")
        lot_filter = next(f for f in s["filters"] if f["filterType"] == "LOT_SIZE")
        tick_size = Decimal(price_filter["tickSize"])
        step_size = Decimal(lot_filter["stepSize"])
        print(f"   tickSize={tick_size}, stepSize={step_size}")
        return tick_size, step_size
    except Exception as e:
        print("❌ Не смогли разобрать filters:", info, "err:", e)
        return None, None


def quantize_to_step(value: float, step: Decimal) -> float:
    """
    Округление вниз до ближайшего кратного step.
    """
    d = Decimal(str(value))
    q = (d / step).to_integral_value(rounding=ROUND_DOWN) * step
    return float(q)


async def test_balance(client: BinanceUSDTMAdapter) -> bool:
    print("💰 Тест /fapi/v2/balance ...")
    try:
        resp = await client._request("GET", "/fapi/v2/balance", {})
    except Exception as e:
        print("❌ Ошибка при запросе /fapi/v2/balance:", repr(e))
        return False

    if not isinstance(resp, list):
        print("⚠️ Нестандартный ответ:", resp)
        return False

    print("✅ Баланс получен, записей:", len(resp))
    if resp:
        print("   Пример записи:", resp[0])
    return True


async def test_order_flow(client: BinanceUSDTMAdapter, base_url: str) -> None:
    """
    Тестовый сценарий:
    - узнаём tickSize/stepSize
    - берём markPrice
    - считаем цену лимитки и qty так, чтобы notional >= 100$
    - создаём ордер и сразу отменяем
    """
    symbol = "BTCUSDT"

    tick_size, step_size = await get_symbol_filters(base_url, symbol)
    if tick_size is None or step_size is None:
        print("⚠️ Не удалось получить tick/step — пропускаем тест ордера")
        return

    connector = aiohttp.TCPConnector(ssl=SSL_CTX)

    print(f"📈 Получаем mark price для {symbol} ...")
    async with aiohttp.ClientSession(connector=connector) as sess:
        async with sess.get(f"{base_url}/fapi/v1/premiumIndex", params={"symbol": symbol}) as resp:
            txt = await resp.text()
            if resp.status != 200:
                print(f"❌ /premiumIndex status={resp.status}, body={txt}")
                return
            try:
                mp = await resp.json()
            except Exception:
                print("❌ /premiumIndex вернул не-JSON:", txt)
                return

    try:
        mark_price = float(mp["markPrice"])
    except Exception as e:
        print("❌ Не смогли распарсить markPrice:", mp, "err:", e)
        return

    print("   markPrice =", mark_price)

    # Чуть ниже рынка, потом квантование по tickSize
    rough_price = mark_price * 0.97
    price = quantize_to_step(rough_price, tick_size)

    # Так как это демо-счёт — просто делаем notional >= 100$ с запасом
    min_notional = 100.0
    min_qty_raw = min_notional / price
    min_qty_down = quantize_to_step(min_qty_raw, step_size)
    qty = min_qty_down + float(step_size)  # добавим шаг сверху, чтобы точно пройти проверку

    print(f"   rough_price={rough_price}, price после квантования={price}")
    print(f"   min_qty_raw={min_qty_raw}, min_qty_down={min_qty_down}, итоговый qty={qty}")

    coid = f"test-{int(time.time())}"

    print(f"📝 Создаём тестовый лимитный ордер BUY {symbol} {qty} по {price}, clientOrderId={coid} ...")

    req = OrderReq(
        symbol=symbol,
        side="BUY",
        type="LIMIT",
        qty=qty,
        price=price,
        time_in_force="GTC",
        reduce_only=False,
        client_order_id=coid,
    )

    try:
        order = await client.create_order(req)
    except Exception as e:
        print("❌ Ошибка при создании ордера:", repr(e))
        return

    print("✅ Ордер создан:", order)

    await asyncio.sleep(1.0)

    print("❌ Отменяем ордер по clientOrderId ...")
    try:
        cancel_resp = await client.cancel_order(symbol=symbol, client_order_id=coid)
    except Exception as e:
        print("❌ Ошибка при отмене ордера:", repr(e))
        return

    print("✅ Ордер отменён:", cancel_resp)


async def async_main():
    load_dotenv()

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    base_url = os.getenv("BINANCE_FUTURES_BASE_URL", "https://demo-fapi.binance.com")

    print("🔑 base_url:", base_url)

    if not api_key or not api_secret:
        print("❌ BINANCE_API_KEY / BINANCE_API_SECRET не найдены в окружении")
        return

    ok_time = await test_server_time(base_url)
    if not ok_time:
        return

    client = BinanceUSDTMAdapter(
        api_key=api_key,
        api_secret=api_secret,
        base_url=base_url,
    )

    try:
        ok_bal = await test_balance(client)
        if not ok_bal:
            return

        await test_order_flow(client, base_url)
    finally:
        try:
            await client.close()
        except Exception:
            pass


def main():
    print(">>> tools.test_binance_testnet: main() called")
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
