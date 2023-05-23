import pandas as pd
import src.constants as constants
from src.config import *
import src.utils as utils


def load_data(sql_server_type):
    module_name = "Load raw data to DB"
    utils.log_print(f"Starting loading raw data to DBase ({sql_server_type})...", module_name=module_name)
    if not utils.if_all_modules_exist(constants.MODULES_PARAM_GROUPS["initdb"]):
        utils.log_print("One of the modules is missing. The program is terminated.", module_name="Load raw data to DB")
    else:
        # Создаём таймер
        timer = utils.Timer("Общий таймер. Загрузка сырых данных.", only_on_show=False)

        # Создаём объект для работы с БД
        import src.db as db
        dh = db.DataHandler(sql_server_type)

        # Получаем информация о загружаемой таблице
        raw_table_name = "Y_TRAIN_RAW"
        file_path = os.path.join(os.path.abspath(os.curdir), CONFIG.RAW_DATA_FULL_PATH, "y_train.parquet")
        df = pd.read_parquet(file_path, engine="fastparquet")

        # Заменим названия столбцов на удобные:
        df.rename(columns=constants.Y_LIST, inplace=True)

        for element in constants.E_LIST:
            # Получим префикс (имя) для эксгаустера
            e_name = element + constants.EXH_SEPARATOR
            # Отберём колонки в данных, которые начинаются с этого имени
            y_columns_list = sorted([one_column for one_column in df.columns if one_column[:len(e_name)] == e_name])
            if len(y_columns_list) != 0:
                timer_element = utils.Timer(f"Эксгаустер: {element}")
                df_2_load = df[y_columns_list]
                # Имя для таблицы:
                table_name = e_name + raw_table_name
                # Загружаем данные
                utils.log_print(f"Загрузка сырых данных y_train для эксгаузера {e_name} в DBase в таблицу {table_name}. Количество строк для загрузки: {df.shape[0]} шт.", module_name=module_name)
                df_2_load.to_sql(table_name, con=dh.connector)
                timer_element.show()

        # Выведем информацию о времени исполнения блока кода:
        timer.show()


print("\n\n\n")
load_data("SQLite")
# print("\n\n\n")
# load_data("PostgreSQL")
