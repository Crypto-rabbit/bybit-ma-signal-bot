# ✅ main.py — единая точка запуска MA-бота

import sys
import os
import asyncio
import webbrowser

from ma_signal_collector import run_bot

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_EXECUTABLE = sys.executable

# ─── Проверка, запущен ли уже экземпляр ────────────────────────────────
LOCK_FILE = os.path.join(BASE_DIR, ".lock")

if os.path.exists(LOCK_FILE):
    print("⚠️ Уже запущено. Закрой предыдущий экземпляр.")
    sys.exit()

with open(LOCK_FILE, "w") as f:
    f.write("running")

# ─── Запуск WebSocket-бота ─────────────────────────────────────────────
async def start_bot():
    try:
        print("🚀 Запуск ma_signal_collector.py ...")
        webbrowser.open("dashboard.html")  # автоматически откроет HTML-интерфейс
        await run_bot()
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")
    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)

if __name__ == "__main__":
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        print("⛔ Остановка по Ctrl+C")
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
