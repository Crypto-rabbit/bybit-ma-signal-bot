# ✅ ma_signal_collector.py — WebSocket-сервер и бот анализа MA

import asyncio
import datetime
import json
import os
from pathlib import Path
from datetime import timezone, timedelta
import logging

from dotenv import load_dotenv
import pandas as pd
import ccxt
import websockets
from itertools import combinations

# ─── ЛОГИРОВАНИЕ ──────────────────────────────────────────────
log_file = Path(__file__).with_name("ma_bot.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ma_bot")

# ─── ЗАГРУЗКА .env ─────────────────────────────────────────────
base = Path(__file__).parent
env_path = base / ".env"
logger.info(f"📁 Папка скрипта: {base}")
logger.info(f"🧾 .env найден: {env_path.exists()}")
load_dotenv(dotenv_path=env_path)

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET  = os.getenv("BYBIT_SECRET")

if not BYBIT_API_KEY or not BYBIT_SECRET:
    logger.critical("❌ Ключи не найдены. Заполните BYBIT_API_KEY и BYBIT_SECRET в .env")
    raise RuntimeError("❌ .env отсутствует или не заполнен")

# ─── НАСТРОЙКИ ────────────────────────────────────────────────
TIMEFRAME = '5m'
LIMIT     = 150

exchange = ccxt.bybit({
    'apiKey': BYBIT_API_KEY,
    'secret': BYBIT_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'},
})

connected = set()

# ─── WebSocket обработчики ─────────────────────────────────────
async def broadcast(message: dict):
    if not connected:
        return
    data = json.dumps(message)
    results = await asyncio.gather(
        *(ws.send(data) for ws in connected),
        return_exceptions=True
    )
    for ws, result in zip(connected, results):
        if isinstance(result, Exception):
            logger.warning(f"Ошибка при отправке WebSocket: {result}")

async def handler(ws):
    connected.add(ws)
    try:
        await ws.wait_closed()
    finally:
        connected.remove(ws)

# ─── Сбор данных ──────────────────────────────────────────────
def fetch_symbols():
    logger.info("📡 Загружаем тикеры с Bybit...")
    try:
        tickers = exchange.fetch_tickers()
    except Exception as e:
        logger.error(f"❌ Ошибка получения тикеров: {e}")
        return []

    symbols = []
    for symbol, data in tickers.items():
        if "/USDT" not in symbol and ":USDT" not in symbol:
            continue
        if not data.get("quoteVolume"):
            continue
        volume_usdt = float(data["quoteVolume"])
        symbols.append((symbol, volume_usdt))

    symbols.sort(key=lambda x: -x[1])
    seen_bases = set()
    unique_symbols = []

    for symbol, volume in symbols:
        base = symbol.split("/")[0]
        if base not in seen_bases:
            seen_bases.add(base)
            unique_symbols.append(symbol)
        if len(unique_symbols) >= 50:
            break

    logger.info(f"✅ Отобраны {len(unique_symbols)} топовых пар по объёму")
    return unique_symbols

def fetch_ohlcv(symbol: str):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=LIMIT)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df
    except Exception as e:
        logger.warning(f"❌ Ошибка загрузки {symbol}: {e}")
        return None

def calculate_ma(df: pd.DataFrame, periods=(9, 18, 36, 72, 144)):
    for period in periods:
        df[f"ma{period}"] = df["close"].rolling(period).mean()
    return df

def detect_signals(df: pd.DataFrame, symbol: str):
    signals = []
    if len(df) < max(144, 20):
        return signals

    prev = df.iloc[-2]
    last = df.iloc[-1]
    ts = last["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
    price = float(last["close"])

    ma_periods = [9, 18, 36, 72, 144]
    ma_pairs = list(combinations(ma_periods, 2))

    for short, long in ma_pairs:
        ma_short_prev = prev[f"ma{short}"]
        ma_long_prev = prev[f"ma{long}"]
        ma_short_last = last[f"ma{short}"]
        ma_long_last = last[f"ma{long}"]

        if ma_short_prev < ma_long_prev and ma_short_last > ma_long_last:
            signals.append({
                "timestamp": ts,
                "symbol": symbol,
                "type": "BUY",
                "price": price,
                "ma": f"MA{short}/MA{long}"
            })
        elif ma_short_prev > ma_long_prev and ma_short_last < ma_long_last:
            signals.append({
                "timestamp": ts,
                "symbol": symbol,
                "type": "SELL",
                "price": price,
                "ma": f"MA{short}/MA{long}"
            })

    return signals

# ─── Основной цикл ────────────────────────────────────────────
async def signal_loop():
    symbols = fetch_symbols()
    if not symbols:
        logger.error("❌ Не удалось получить список символов. Остановка.")
        return

    logger.info(f"📈 Мониторинг {len(symbols)} топовых пар на Bybit...")

    while True:
        now = datetime.datetime.now(timezone.utc)
        next_check = (now + timedelta(minutes=5)).replace(second=0, microsecond=0)
        next_check = next_check.replace(minute=(next_check.minute // 5) * 5)
        wait_seconds = (next_check - now).total_seconds()

        logger.info(f"🕒 Следующая проверка в {next_check.strftime('%H:%M:%S')} UTC (через {int(wait_seconds)} секунд)")
        await asyncio.sleep(wait_seconds)

        for symbol in symbols:
            df = fetch_ohlcv(symbol)
            ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            price = df["close"].iloc[-1] if df is not None and not df.empty else "-"

            if df is None:
                await broadcast({"timestamp": ts, "symbol": symbol, "type": "❌ Ошибка загрузки", "price": price})
                continue

            df = calculate_ma(df)
            signals = detect_signals(df, symbol)

            if signals:
                for sig in signals:
                    await broadcast(sig)
            else:
                await broadcast({"timestamp": ts, "symbol": symbol, "type": "✅ Нет сигнала", "price": price})

        logger.info("✅ Цикл проверки завершён\n")

# ─── Запуск ───────────────────────────────────────────────────
async def main():
    logger.info("🚀 ma_signal_collector запущен")
    try:
        server = await websockets.serve(handler, "localhost", 6789)
        logger.info("📡 WebSocket сервер работает на ws://localhost:6789")
    except Exception as e:
        logger.critical(f"❌ WebSocket не удалось запустить: {e}")
        return

    await signal_loop()

# 👉 Обёртка для запуска из main.py
async def run_bot():
    await main()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"❌ Фатальная ошибка: {e}")