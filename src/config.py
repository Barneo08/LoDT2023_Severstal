import os
import logging


class FlaskConfiguration(object):
    DEBUG = True


if os.path.isfile("_local_config.py"):
    from _local_config import *
else:
    class CommonConfiguration(object):
        DEBUG = True
        DB_FULL_PATH = "DBase"
        RAW_DATA_FULL_PATH = "indata"
        X_TRAIN_FILE = "x_train.parquet"
        Y_TRAIN_FILE = "y_train.parquet"
        X_TEST_FILE = "x_test.parquet"
        MODELS_FULL_PATH = "Models"

        CREATE_NAN_FLAGS = True

        SQL_SERVER_TYPE_USED = "Pandas"  # (SQLite, PostgreSQL, Pandas)
        SQL_SERVER_DB_NAME = "SeverStal_DB"

        PostgreSQL = {
            "SQL_SERVER_HOST": "localhost",
            "SQL_SERVER_USERNAME": "postgres",
            "SQL_SERVER_PASSWORD": "123",
        }

        UPLOAD_ROWS = 10_000

        MINUTES_ROWS_IN_GROUP = 10  # Сколько минут, идущих подряд группировать в одну строку
        ROWS_IMMERSION_DEPTH = int(5 * 60 / 10)  # С какой глубины поднимать строки на уровень текущей записи
        HOURS_FORECAST_HORIZON = 3  # Горизонт планирования в часах (суммируется с минутами)
        MINUTES_FORECAST_HORIZON = 10  # Горизонт планирования в минутах (суммируется с часами)
        SAVE_ONLY_STABLE_AND_FIRST_ERROR = False  # В этом случае из Y удаляются все записи, идущие сразу после того как возникла ошибка.

        RAW_FILES_UPLOAD_ENABLED = True
        DB_READY = False

        def __init__(self):
            if self.DEBUG:
                debug_logs_path = os.path.join(os.path.abspath(os.curdir), "debug.logs")

                if not os.path.isdir(debug_logs_path):
                    os.mkdir(debug_logs_path)

                log_file_name = os.path.join(debug_logs_path, "debug.log")
                logging.basicConfig(filename=log_file_name, level=logging.DEBUG)


CONFIG = CommonConfiguration()

__ALL__ = [
    "CONFIG",
 ]