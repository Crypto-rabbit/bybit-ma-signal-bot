import subprocess
import webbrowser
import time
import os
import sys

LOCK_FILE = "start.lock"

if os.path.exists(LOCK_FILE):
    print(" Уже запущено. Закрой предыдущий экземпляр.")
    sys.exit()

with open(LOCK_FILE, "w") as f:
    f.write("locked")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_EXECUTABLE = sys.executable

#  Путь к логам
bot_log_path = os.path.join(BASE_DIR, "ma_bot.log")
dashboard_log_path = os.path.join(BASE_DIR, "dashboard.log")

#  Запуск основного бота с логами
print(" Запуск ma_signal_collector.py ...")
with open(bot_log_path, "w", encoding="utf-8") as log_file:
    subprocess.Popen(
        [PYTHON_EXECUTABLE, os.path.join(BASE_DIR, "ma_signal_collector.py")],
        stdout=log_file,
        stderr=log_file
    )

#  Подождать 2 секунды
time.sleep(2)

#  Запуск Dashboard
print(" Запуск dashboard.py ...")
with open(dashboard_log_path, "w", encoding="utf-8") as dash_log:
    subprocess.Popen(
        [PYTHON_EXECUTABLE, os.path.join(BASE_DIR, "dashboard.py")],
        stdout=dash_log,
        stderr=dash_log
    )

#  Подождать 3 секунды
time.sleep(3)

#  Открыть в браузере
webbrowser.open("http://localhost:8000")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print(" Завершение работы вручную.")
finally:
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
    print(" Блокировка снята. Выход.")
