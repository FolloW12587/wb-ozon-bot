import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "app.log")

formatter = logging.Formatter(
    fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

handler = TimedRotatingFileHandler(
    LOG_FILE,
    when="W0",  # раз в неделю (в понедельник)
    interval=1,
    backupCount=10,  # храним 10 недель логов
    encoding="utf-8",
)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)

logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Также выводим в консоль (опционально)
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)
