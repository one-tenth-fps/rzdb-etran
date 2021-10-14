import yaml

with open("config.yaml", encoding="utf8") as f:
    config = yaml.safe_load(f)

QUEUE_MAXSIZE = config["app"]["QUEUE_MAXSIZE"]
WORKERS_COUNT = config["app"]["WORKERS_COUNT"]
SLEEP_ON_DISCONNECT = config["app"]["SLEEP_ON_DISCONNECT"]
SLEEP_ON_DOS = config["app"]["SLEEP_ON_DOS"]
SLEEP_ON_DOS_MAX = config["app"]["SLEEP_ON_DOS_MAX"]
DB_POLLING_INTERVAL = config["app"]["DB_POLLING_INTERVAL"]
DB_QUERYING_INTERVAL = config["app"]["DB_QUERYING_INTERVAL"]
REQUEST_TIMEOUT = config["app"]["REQUEST_TIMEOUT"]
HEARTBEAT_INTERVAL = config["app"]["HEARTBEAT_INTERVAL"]
HEARTBEAT_PATH = config["app"]["HEARTBEAT_PATH"]
SERVICE_NAME = config["app"]["SERVICE_NAME"]
HTTP_ENDPOINT_PORT = config["app"]["HTTP_ENDPOINT_PORT"]
DEBUG = config["app"]["DEBUG"]

DB_DRIVER = config["db"]["driver"]
DB_SERVER = config["db"]["server"]
DB_USER = config["db"]["user"]
DB_PASSWORD = config["db"]["password"]
DB_DATABASE = config["db"]["database"]
DB_CONNECTION_STRING = f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_DATABASE};UID={DB_USER};PWD={DB_PASSWORD}"

ETRAN_LOGIN = config["etran"]["login"]
ETRAN_PASSWORD = config["etran"]["password"]
ETRAN_URL = config["etran"]["url"]
ETRAN_HEADERS = config["etran"]["headers"]
ETRAN_GZIP = config["etran"]["gzip"]
