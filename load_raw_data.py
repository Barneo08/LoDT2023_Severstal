import pandas as pd
import src.constants as constants
from src.config import *
import src.utils as utils
from tqdm import tqdm


def load_data(sql_server_type):
    module_name = "Load raw data to DB"
    utils.log_print(f"Starting loading raw data to DBase ({sql_server_type})...", module_name=module_name)
    if not utils.if_all_modules_exist(constants.MODULES_PARAM_GROUPS["initdb"]):
        utils.log_print("One of the modules is missing. The program is terminated.", module_name="Load raw data to DB")
    else:
        # Создаём объект для работы с БД
        import src.db as db
        dh = db.DataHandler(sql_server_type)

        # Получаем информация о загружаемой таблице
        raw_table_name = "Y_TRAIN_RAW"
        file_path = os.path.join(os.path.abspath(os.curdir), CONFIG.RAW_DATA_FULL_PATH, "y_train.parquet")
        df = pd.read_parquet(file_path, engine="fastparquet")
        # df = df.head(99)

        # Заменим названия столбцов на удобные:
        df.rename(columns=constants.Y_LIST, inplace=True)

        # Создаём таймер
        timer = utils.Timer("Общий таймер. Загрузка сырых данных.", only_on_show=False)

        rows = df.shape[0]
        print(f"{raw_table_name}:")
        print(f"Количество строк для загрузки: {utils.sep_digits(rows)} шт.")
        print(f"Загрузка осуществляется блоками по {utils.sep_digits(CONFIG.UPLOAD_ROWS)} строк.")

        for element in constants.E_LIST:
            # Получим префикс (имя) для эксгаустера
            e_name = element + constants.EXH_SEPARATOR
            # Отберём колонки в данных, которые начинаются с этого имени
            y_columns_list = sorted([one_column for one_column in df.columns if one_column[:len(e_name)] == e_name])
            if len(y_columns_list) != 0:
                # timer_element = utils.Timer(f"Эксгаустер: {element}")
                df_2_load = df[y_columns_list]
                # Имя для таблицы:
                table_name = e_name + raw_table_name
                # Загружаем данные
                utils.log_print(f"Загрузка сырых данных y_train для эксгаузера {element} в DBase в таблицу {table_name}.", module_name=module_name, to_screen=False)

                # Если таблица есть, то её надо удалить.
                dh.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                dh.commit()

                # Загрузим данные частями по UPLOAD_ROWS
                def data_2_sql(num1, num2):
                    df_2_load.iloc[num1: num2].to_sql(table_name, con=dh.connector, if_exists="append")

                for num in tqdm(range(int(rows / CONFIG.UPLOAD_ROWS) + 1), ncols=75, desc=f"Эксгаузер: {element}"):
                    data_2_sql(num * CONFIG.UPLOAD_ROWS, (num * CONFIG.UPLOAD_ROWS) + CONFIG.UPLOAD_ROWS)


        # Выведем информацию о времени исполнения блока кода:
        timer.show()


print("\n\n")
load_data("SQLite")

print("\n\n\n")
load_data("PostgreSQL")

print("\n\n")
load_data("SQLite+SQLAlchemy")
