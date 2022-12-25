try:
    import local_secrets as secrets
except ImportError:
    raise FileNotFoundError('You will need to create a local secrets file first!')

DB_NAME = secrets.DB_NAME
DB_HOST = secrets.DB_HOST
DB_USER = secrets.DB_USER
DB_PWD = secrets.DB_PWD
DB_PORT = secrets.DB_PORT
