import yaml

with open('config.yaml', encoding='utf8') as f:
    config = yaml.safe_load(f)

QUEUE_MAXSIZE = config['app']['QUEUE_MAXSIZE']
WORKERS_COUNT = config['app']['WORKERS_COUNT']
SLEEP_ON_DISCONNECT = config['app']['SLEEP_ON_DISCONNECT']
SLEEP_ON_DOS = config['app']['SLEEP_ON_DOS']
SLEEP_ON_DOS_MAX = config['app']['SLEEP_ON_DOS_MAX']
DB_POLLING_INTERVAL = config['app']['DB_POLLING_INTERVAL']
DEBUG = config['app']['DEBUG']

db_driver = config['db']['driver']
db_server = config['db']['server']
db_user = config['db']['user']
db_password = config['db']['password']
db_database = config['db']['database']
db_connection_string = f'DRIVER={db_driver};SERVER={db_server};DATABASE={db_database};UID={db_user};PWD={db_password}'

etran_login = config['etran']['login']
etran_password = config['etran']['password']
etran_url = config['etran']['url']
etran_headers = config['etran']['headers']