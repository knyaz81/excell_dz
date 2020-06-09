import os.path

## Program modes
ACTIONS = [
    'generate',
    'parse',
]

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

## Postgres
DB_CONFIG = {
    'USER': 'excellparser',
    'PASSWORD': 'excellparser',
    'NAME': 'excellparser',
    'INITDB_NAME': 'template1',
    'HOST': 'localhost',
    'PORT': 5433,
}
