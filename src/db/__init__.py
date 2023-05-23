import pandas as pd
from src.config import *
import src.constants as constants
import src.utils as utils

import sqlite3 as sqlite
# (sqlite.Error, sqlite.ProgrammingError, sqlite.DatabaseError, sqlite.IntegrityError, sqlite.OperationalError, sqlite.NotSupportedError)
import psycopg2
from sqlalchemy import create_engine


class DataHandler:
    """
    При создании экземпляра данного класса будет создан объект,
    который позволяет управлять таблицами базы данных.
    При его создании если база данных отсутствует, то будет создана,
    а справочники заполнены.
    """
    connector = None

    def __init__(self, sql_server="SQLite"):
        """
        Объект класса позволяет работать с базой данных.
        """
        self.sql_server = sql_server
        self.get_connector()
        self.create_tables()
        
        # Добавим данные в справочники.
        # Пример для справочника:
        # initial_data = constants.initial_data_PERIOD_TYPES
        # self.add_rows_from_struct(initial_data)

        # self.add_rows_from_struct()

    @staticmethod
    def get_dbase_path():
        try:
            if not os.path.isdir(CONFIG.DB_FULL_PATH):
                os.makedirs(CONFIG.DB_FULL_PATH)

            ret_path = os.path.join(CONFIG.DB_FULL_PATH, CONFIG.SQL_SERVER_DB_NAME)
            return ret_path
        except OSError:
            print("Не удалось подготовить информацию о расположении базы данных.")
            return ""

    def __del__(self):
        """
        При уничтожении экземпляра класса
        закрывается соединение с базой данных.
        """
        self.close_connection()
        print("Закрыли соединение с БД.")

    def get_connector(self):
        """
        Подключение к базе данных.
        """
        try:
            if self.sql_server.upper() == "SQLite".upper():
                self.connector = sqlite.connect(self.get_dbase_path(), check_same_thread=False)
            elif self.sql_server.upper() == "PostgreSQL".upper():
                engine_data = f"{CONFIG.SQL_SERVER_USERNAME}:{CONFIG.SQL_SERVER_PASSWORD}@{CONFIG.SQL_SERVER_HOST}/{CONFIG.SQL_SERVER_DB_NAME}"
                self.connector = create_engine(f"postgresql+psycopg2://{engine_data}")
        except sqlite.Error:
            print("get_connector: Возникла проблема с подключением к базе данных.")
            if self.connector:
                self.close_connection()
            return False
        if self.connector:
            print("Установили соединение с БД.")
            return True
        else:
            print("get_connector: При установлении связи с БД возникли ошибки.")
            return False

    def close_connection(self):
        if self.sql_server.upper() == "SQLite".upper():
            self.connector.close()
        elif self.sql_server.upper() == "PostgreSQL".upper():
            pass

    def commit(self):
        self.connector.commit()

    def rollback(self):
        self.connector.rollback()

    def create_tables(self):
        """
        Создание основных таблиц, их индексов, триггеров и заполнение справочников.
        """
        if not self.connector:
            return False
        
        # cur = self.connector.cursor()

        # table_name = ""
        # try:
        #     cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (mfd_id INTEGER NOT NULL, price_dt TIMESTAMP NOT NULL, id_period_type INTEGER NOT NULL, price_open REAL NOT NULL, price_min REAL NOT NULL, price_max REAL NOT NULL, price_close REAL NOT NULL, vol INTEGER NOT NULL, ppredict INTEGER NOT NULL)")
        #     cur.execute("CREATE TRIGGER IF NOT EXISTS trig_STOCKS_PRICES_befor_insert BEFORE INSERT ON STOCKS_PRICES FOR EACH ROW BEGIN DELETE FROM STOCKS_PRICES WHERE mfd_id=NEW.mfd_id AND price_dt=NEW.price_dt AND id_period_type=NEW.id_period_type; END")
        #     cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS unique_combination ON STOCKS_PRICES (mfd_id, price_dt, id_period_type)")
        #     cur.execute("CREATE INDEX IF NOT EXISTS ppredict ON STOCKS_PRICES (ppredict)")
        # except:
        #     utils.log_print(f"create_tables: При создании таблицы {table_name} возникла ошибка.", module_name="intDB")
        #     return False
        # Так как ошибок не было, то выходим с True
        return True

    def add_row(self, table_name, data, donot_commit=False):
        """
        Данная функция предназначена для ввода данных по одной строке.
        Данные добавляются в таблице имя которой передано в параметре <table_name>.
        Имя столбца идентификатора в таблице передается в параметре <id_column>.
        Если параметр <find_and_update_or_insert>= False тогда переданные данные добавляются как новая строка.
        Если параметр <find_and_update_or_insert>= True тогда сначала делается попытка
        найти строку с соответствующим идентификатором. Если такая строка найдена тогда она обновляется,
        а если не найдена тогда добавляется новая строка.
        Если параметр <donot_commit>=True тогда по завершении метода commit не делается.

        !!! Важно !!! Здесь далее приведён пример использования.

        """
        if not self.connector:
            print("add_row: У данного объекта отсутствует коннектор.")
            return False

        table_name = table_name.replace(" ", "").upper()
        cur = self.connector.cursor()

        if True:
            if table_name == "Y_TRAIN_RAW":
                sql_create_string = f"INSERT INTO {table_name} (DT ##ALL_OTHER##) VALUES(##?##)"

                sql_help_str = ""
                insert_data = [data["DT"]]
                for element in constants.Y_LIST.keys():
                    sql_help_str = sql_help_str + f", {constants.Y_LIST[element]}"
                    insert_data.append(data[constants.Y_LIST[element]])

                sql_create_string = sql_create_string.replace("##ALL_OTHER##", sql_help_str)
                sql_create_string = sql_create_string.replace("##?##", "?" + ", ?" * sql_help_str.count(","))
                insert_data = tuple(insert_data)

                cur.execute(sql_create_string, insert_data)

            elif table_name == "STOCKS_PRICES":
                cur.execute("INSERT INTO STOCKS_PRICES (mfd_id, price_dt, id_period_type, price_open, price_min, price_max, price_close, vol, ppredict) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (data["mfd_id"], data["price_dt"], data["id_period_type"], data["price_open"], data["price_min"], data["price_max"], data["price_close"], data["vol"], data["ppredict"]))
            else:
                print("add_row: Попытка добавить данные в несуществующую таблицу: " + table_name)
                return False
        try:
            pass
        except (sqlite.Error, sqlite.ProgrammingError, sqlite.DatabaseError, sqlite.IntegrityError, sqlite.OperationalError, sqlite.NotSupportedError):
            print(f"add_row: При добавлении строки в таблицу {table_name} возникла ошибка.")
            if not donot_commit:
                self.connector.rollback()

            return False

        if not donot_commit:
            self.connector.commit()
        # Так как ошибок не было, то выходим с True
        return True

    def get_stocks_prices_pd(self, mfd_id, id_period_type=constants.DEFAULT_PERIOD_TYPE, price_type="MAX", dt_begin="", dt_end="", ppredict=0):
        """
        Выборка цен на акцию по условию и предоставление данных в формате Pandas DataFrame.
        """
        if not self.connector:
            return False
        cur = self.connector.cursor()
        try:
            cur.execute("SELECT price_dt, price_{} AS price, vol FROM STOCKS_PRICES WHERE mfd_id=? and id_period_type=? AND ppredict=? AND price_dt BETWEEN ? AND ? ORDER BY price_dt".format(price_type.lower()), (mfd_id, id_period_type, ppredict, dt_begin, dt_end))
            return pd.DataFrame(cur.fetchall(), columns=["price_dt", "price", "vol"])
        except BaseException:
            print("get_stocks_list: При попытке получить выборку из таблицы STOCKS_PRICES возникла ошибка.")
            return False

    @staticmethod
    def load_stock_prises_from_file2df(file_name):
        """
        Загрузка информации о ценах из локального файла в формат Pandas DataFrame.
        """
        if not os.path.isfile(file_name):
            print("load_stock_prises_from_file: Файл '{}' не найден.".format(file_name))
            return False

        print("Получаем данные о стоимости акций из текстового файла '{}'".format(file_name))
        df = pd.read_csv(file_name, sep=";")
        # Полученную таблицу нужно причесать:
        return utils.prepare_df(df)
