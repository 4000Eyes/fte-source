import os

# uncomment the line below for postgres database url from environment variable
# postgres_local_base = os.environ['DATABASE_URL']

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'my_precious_secret_key')
    DEBUG = False


class DevelopmentConfig(Config):
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_main.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JWT_SECRET_KEY = 'T39FHGYA87ERedsk743023mnb'
    FTEYES_DB = 'fourthousandeyes'
    FTEYES_HOST = "mongodb://127.0.0.1"
    FTEYES_PORT = 27017
    FTEYES_USERNAME = "fteyesuser"
    FTEYES_PASSWORD = "Gundan123@"
    FTEYES_GDB_URI = "bolt://localhost:11006"
    FTEYES_GDB_USER = "krisraman"
    FTEYES_GDB_PWD = "Gundan123@"
    FTEYES_GDB_DB = "fourthousandeyes"
    # Logging Setup
    LOG_TYPE = "watched"  # Default is a Stream handler
    LOG_LEVEL = "INFO"
    REDIS_HOST="127.0.0.1"
    REDIS_PORT=6379
    REDIS_PASSWORD="rajuvedu123@"
    REDIS_DBNAME=0

    # File Logging Setup
    LOG_DIR = "/home/krissrinivasan/python/logs"
    APP_LOG_NAME = "app.log"
    WWW_LOG_NAME = "www.log"
    LOG_MAX_BYTES = 100_000_000  # 100MB in bytes
    LOG_COPIES = 5


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_test.db')
    PRESERVE_CONTEXT_ON_EXCEPTION = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JWT_SECRET_KEY = 'T39FHGYA87ERedsk743023mnb'
    FTEYES_DB = 'fourthousandeyes'
    FTEYES_HOST = "mongodb://127.0.0.1"
    FTEYES_PORT = 27017
    FTEYES_USERNAME = "fteyesuser"
    FTEYES_PASSWORD = "Gundan123@"
    FTEYES_GDB_URI = "bolt://localhost:11006"
    FTEYES_GDB_USER = "krisraman"
    FTEYES_GDB_PWD = "Gundan123@"
    FTEYES_GDB_DB = "fourthousandeyes"
    # Logging Setup
    LOG_TYPE = "watched"  # Default is a Stream handler
    LOG_LEVEL = "INFO"
    REDIS_HOST="127.0.0.1"
    REDIS_PORT=6379
    REDIS_PASSWORD="rajuvedu123@"
    REDIS_DBNAME=0
    # File Logging Setup
    LOG_DIR = "/home/krissrinivasan/python/logs"
    APP_LOG_NAME = "app.log"
    WWW_LOG_NAME = "www.log"
    LOG_MAX_BYTES = 100_000_000  # 100MB in bytes
    LOG_COPIES = 5


class ProductionConfig(Config):
    DEBUG = False
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base
    JWT_SECRET_KEY = 'T39FHGYA87ERedsk743023mnb'
    FTEYES_DB = 'fourthousandeyes'
    FTEYES_HOST = "mongodb://127.0.0.1"
    FTEYES_PORT = 27017
    FTEYES_USERNAME = "fteyesuser"
    FTEYES_PASSWORD = "Gundan123@"
    FTEYES_GDB_URI = "bolt://localhost:11006"
    FTEYES_GDB_USER = "krisraman"
    FTEYES_GDB_PWD = "Gundan123@"
    FTEYES_GDB_DB = "fourthousandeyes"
    # Logging Setup
    LOG_TYPE = "watched"  # Default is a Stream handler
    LOG_LEVEL = "INFO"
    REDIS_HOST="127.0.0.1"
    REDIS_PORT=6379
    REDIS_PASSWORD="rajuvedu123@"
    REDIS_DBNAME=0
    # File Logging Setup
    LOG_DIR = "/home/krissrinivasan/python/logs"
    APP_LOG_NAME = "app.log"
    WWW_LOG_NAME = "www.log"
    LOG_MAX_BYTES = 100_000_000  # 100MB in bytes
    LOG_COPIES = 5


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY
