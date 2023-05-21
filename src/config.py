import os
import logging


class FlaskConfiguration(object):
    DEBUG = True


class CommonConfiguration(object):
    DEBUG = True
    DB_FULL_PATH = ""

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