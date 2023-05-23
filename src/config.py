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

        SQL_SERVER_TYPE_USED = "SQLite"  # (SQLite, PostgreSQL)
        SQL_SERVER_DB_NAME = "SeverStal_DB"

        PostgreSQL = {
            "SQL_SERVER_HOST": "localhost",
            "SQL_SERVER_USERNAME": "postgres",
            "SQL_SERVER_PASSWORD": "123",
        }

        UPLOAD_ROWS = 1_000_000
        RAW_FILES_UPLOAD_ENABLED = False
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