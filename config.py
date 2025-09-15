import os

from dotenv import load_dotenv

from sqlalchemy.engine import URL


load_dotenv()

DEBUG = os.environ.get("DEBUG", "0") == "1"
DEV_ID = os.environ.get("DEV_ID")
SUB_DEV_ID = os.environ.get("SUB_DEV_ID")

TOKEN = os.environ.get("TOKEN")
WEBAPP_URL_ONE = os.environ.get("WEBAPP_URL_ONE")
WEBAPP_URL_TWO = os.environ.get("WEBAPP_URL_TWO")
WEBAPP_URL_THREE = os.environ.get("WEBAPP_URL_THREE")

HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")

PUBLIC_URL = os.environ.get("PUBLIC_URL")


# DATABASE
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")
DB_HOST = os.environ.get("POSTGRES_HOST")
DB_PORT = os.environ.get("POSTGRES_PORT")
DB_NAME = os.environ.get("POSTGRES_DB")

JOB_STORE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

PGBOUNCER_HOST = os.environ.get("PGBOUNCER_HOST")


db_url = URL.create(
    "postgresql+asyncpg",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

_db_url = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)


DUMP_CHAT = os.getenv("DUMP_CHAT")
ADMIN_IDS = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = list(map(int, ADMIN_IDS.split(","))) if ADMIN_IDS else []
ADMINS_CHAT_ID = os.getenv("PAYMENTS_CHAT_ID")

# Redis
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")


# Bearer authentication token
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")


FEEDBACK_REASON_PREFIX = "feedback_reason"

# Yandex metrika
COUNTER_ID = os.environ.get("COUNTER_ID")
YANDEX_TOKEN = os.environ.get("YANDEX_TOKEN")


# API URL`s
WB_API_URL = os.environ.get("WB_API_URL")
OZON_API_URL = os.environ.get("OZON_API_URL")
API_SERVICES_TIMEOUT = 35
WB_DEFAULT_DELIVERY_ZONE = -1281648


FAKE_NOTIFICATION_SECRET = os.environ.get("FAKE_NOTIFICATION_SECRET")


# PATHS
STATIC_DIR = "./static"
IMAGES_DIR = os.path.join(STATIC_DIR, "img")

DATA_DIR = "./data"
IMAGES_CONFIG_PATH = os.path.join(DATA_DIR, "images.json")


# Yoomoney
YOOMONEY_RECEIVER = os.environ.get("YOOMONEY_RECEIVER")
YOOMONEY_NOTIFICATION_SECRET = os.environ.get("YOOMONEY_NOTIFICATION_SECRET")


SUPPORT_BOT_URL = "https://t.me/NaSkidku_support"
