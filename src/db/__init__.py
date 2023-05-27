import pandas as pd
from src.config import *
import src.constants as constants
import src.utils as utils

import sqlite3 as sqlite
# (sqlite.Error, sqlite.ProgrammingError, sqlite.DatabaseError, sqlite.IntegrityError, sqlite.OperationalError, sqlite.NotSupportedError)
from sqlalchemy import create_engine, text

from datetime import datetime, timedelta
import random


class PandasClass:
    db_path = None

    def __init__(self, db_path):
        self.db_path = db_path

    def close(self):
        pass

    def to_file(self, df: pd.DataFrame, table_name):
        save_file_ext = ".parquet"
        if table_name[-len(save_file_ext):] == save_file_ext:
            save_file_ext = ""

        df.to_parquet(f"{os.path.join(self.db_path, table_name)}{save_file_ext}", engine='fastparquet', index=True)

    def get_file_path(self, exh_id, type_data="", type_for="train"):
        if type_data.upper().replace(" ", "") == "X":
            file_type_data = "X"
        elif type_data.upper().replace(" ", "") == "Y":
            file_type_data = "Y"
        else:
            file_type_data = "Error FILE_TYPE_NAME"

        if type_for == "train":
            file_type_name = "TRAIN"
        elif type_for == "test":
            file_type_name = "TEST"
        else:
            file_type_name = "Error TYPE_FOR"

        exh_id = exh_id.upper().replace(" ", "")
        table_name = f"{exh_id}_{file_type_data}_{file_type_name}_GROUPED.parquet"
        ret_file_name = f"{os.path.join(self.db_path, table_name)}"

        return ret_file_name

    @staticmethod
    def cursor():
        return None

    def commit(self):
        pass

    def rollback(self):
        pass

    def execute(self, sql_command):
        pass


class DataHandler:
    """
    При создании экземпляра данного класса будет создан объект,
    который позволяет управлять таблицами базы данных.
    При его создании если база данных отсутствует, то будет создана,
    а справочники заполнены.
    """
    connector = None
    engine = None
    sql_alchemy = None
    keep_silence = None
    cursor = None

    def __init__(self, sql_server=None, keep_silence=True):
        """
        Объект класса позволяет работать с базой данных.
        """
        self.keep_silence = keep_silence
        if CONFIG.DB_READY:
            self.sql_server = CONFIG.SQL_SERVER_TYPE_USED
        else:
            if sql_server is None:
                utils.log_print("Не удалось подготовить информацию о расположении базы данных.", module_name="intDB")
                return
            else:
                self.sql_server = sql_server

        self.get_handle()
        self.create_tables()
        
        # Добавим данные в справочники.
        # Пример для справочника:
        # initial_data = constants.initial_data_PERIOD_TYPES
        # self.add_rows_from_struct(initial_data)

        # self.add_rows_from_struct()

    @staticmethod
    def get_dbase_path(with_db_name=True):
        try:
            dbase_path = os.path.abspath(CONFIG.DB_FULL_PATH)
            if not os.path.isdir(dbase_path):
                os.makedirs(dbase_path)

            if with_db_name:
                ret_path = os.path.join(dbase_path, CONFIG.SQL_SERVER_DB_NAME)
            else:
                ret_path = dbase_path

            return ret_path
        except OSError:
            utils.log_print("Не удалось подготовить информацию о расположении данных.", module_name="get_dbase_path")
            return ""

    def __del__(self):
        """
        При уничтожении экземпляра класса
        закрывается соединение с базой данных.
        """
        if self.connector:
            self.close_handle()
            if not self.keep_silence:
                print("Закрыли соединение с данными.")

    def get_handle(self):
        """
        Подключение к базе данных.
        """
        try:
            if self.sql_server.upper() == "Pandas".upper():
                self.sql_alchemy = False
                self.connector = PandasClass(self.get_dbase_path(with_db_name=False))
            elif self.sql_server.upper() == "SQLite".upper():
                self.sql_alchemy = False
                self.connector = sqlite.connect(self.get_dbase_path(), check_same_thread=False)
            elif self.sql_server.upper() == "SQLite+SQLAlchemy".upper():
                self.sql_alchemy = True
                engine_data = self.get_dbase_path()
                self.engine = create_engine(f"sqlite:///{engine_data}")
                self.connector = self.engine.connect()
            elif self.sql_server.upper() == "PostgreSQL".upper():
                self.sql_alchemy = True
                user_name = CONFIG.PostgreSQL["SQL_SERVER_USERNAME"]
                password = CONFIG.PostgreSQL["SQL_SERVER_PASSWORD"]
                host = CONFIG.PostgreSQL["SQL_SERVER_HOST"]
                db_name = CONFIG.SQL_SERVER_DB_NAME
                engine_data = f"{user_name}:{password}@{host}/{db_name}"
                self.engine = create_engine(f"postgresql+psycopg2://{engine_data}")
                self.connector = self.engine.connect()

            self.cursor = self.connector.cursor()
        except BaseException:
            utils.log_print("get_connector: Возникла проблема с подключением к данным.", module_name="get_handle")
            if self.connector is None or self.engine is None:
                self.close_handle()
            return False
        if self.connector or self.engine:
            if not self.keep_silence:
                print("Подключились к данным.")
            return True
        else:
            utils.log_print("get_connector: При установлении связи с данными возникли ошибки.", module_name="get_handle")
            return False

    def execute(self, sql_command):
        if self.connector:
            if self.sql_alchemy:
                self.connector.execute(text(sql_command))
            else:
                self.connector.execute(sql_command)

    def close_handle(self):
        if self.connector:
            self.connector.close()

    def commit(self):
        if self.connector:
            self.connector.commit()

    def rollback(self):
        if self.connector:
            self.connector.rollback()

    @staticmethod
    def get_exh_list():
        """
        Возвращает словарь со списком эксгаутеров в качестве ключей
        и их идентификаторов в качестве значений
        """
        ret_list = []
        exh_list = sorted(constants.E_DICT.keys())
        for exh in exh_list:
            ret_list.append({"name": exh, "id": constants.E_DICT[exh]})

        return ret_list

    @staticmethod
    def get_exh_tp_list(exh_id=None):
        if exh_id is None:
            return {"НЕ ПЕРЕДАН ИДЕНТИФИКАТОР ЭКСГАУСТЕРА": "НЕ ПЕРЕДАН ИДЕНТИФИКАТОР ЭКСГАУСТЕРА"}

        ret_dict = {"Names": [], "IDs": []}
        tp_list = sorted(constants.Y_LIST.keys())
        for key in tp_list:
            if constants.Y_LIST[key][:len(exh_id)] == exh_id:
                ret_dict["Names"].append(key)
                ret_dict["IDs"].append(constants.Y_LIST[key])

        return ret_dict

    @staticmethod
    def get_exh_events(exh_tp_id=None, dt_start=(datetime.now() - timedelta(hours=1)), dt_end=datetime.now()):
        if exh_tp_id is None:
            print("Необходимо указать идентификатор технического места.")

        minutes = int((dt_end - dt_start).total_seconds() / 60)
        m1 = sorted(random.sample(range(1, minutes), random.randint(1, 3)))
        m3 = sorted(random.sample(range(1, minutes), random.randint(3, 20)))

        def get_dt_sequence(dt_begin, minutes_list):
            ret_list = []
            for one_minute in minutes_list:
                ret_list.append(dt_begin + timedelta(minutes=one_minute))
            return ret_list

        ret_dict = {
            "M1": {"dt": get_dt_sequence(dt_start, m1)},
            "M3": {"dt": get_dt_sequence(dt_start, m3)},
        }

        return ret_dict

    def create_tables(self):
        """
        Создание основных таблиц, их индексов, триггеров и заполнение справочников.
        """
        if not self.connector:
            return False
        
        return True

