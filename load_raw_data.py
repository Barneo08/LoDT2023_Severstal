import sys
import pandas as pd
from tqdm import tqdm

from src.config import *
import src.constants as constants
import src.utils as utils


def load_data(sql_server_type):
    module_name = "Load raw data to DB"

    if not CONFIG.RAW_FILES_UPLOAD_ENABLED:
        print("-------------------------------------------------------------------")
        utils.log_print("Uploading raw files is blocked in the settings.", module_name=module_name)
        print("-------------------------------------------------------------------")
        print()
        print("Terminated.")
        return

    return
    utils.log_print(f"Starting loading raw data to DBase ({sql_server_type})...", module_name=module_name)
    if not utils.if_all_modules_exist(constants.MODULES_PARAM_GROUPS["initdb"]):
        utils.log_print("One of the modules is missing. The program is terminated.", module_name="Load raw data to DB")
    else:
        # Создаём объект для работы с БД
        import src.db as db
        dh = db.DataHandler(sql_server_type)

        conf = {
            "X_TRAIN_RAW": {"raw_data_file": "x_train.parquet", "columns_subs": constants.SENSOR_FIELD_NAMES_LIST},
            "Y_TRAIN_RAW": {"raw_data_file": "y_train.parquet", "columns_subs": constants.Y_LIST},
        }

        for raw_table_name in conf.keys():
            # Получаем информация о загружаемой таблице
            # raw_table_name = "Y_TRAIN_RAW"
            file_path = os.path.join(os.path.abspath(os.curdir), CONFIG.RAW_DATA_FULL_PATH, conf[raw_table_name]["raw_data_file"])
            df = pd.read_parquet(file_path, engine="fastparquet")
            # df = df.head(99)

            # Заменим названия столбцов на удобные:
            df.rename(columns=conf[raw_table_name]["columns_subs"], inplace=True)

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
                y_columns_list = sorted(y_columns_list)
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


if len(sys.argv) == 1:
    print("--------------------------------------------")
    print("Are you sure want to upload the raw data")
    print("to the database for further processing?")
    print()
    print("By default, the data will be loaded to the SQLite.")
    print("You can select PostgreSQL if you run the program with:")
    print("--server PostgreSQL")
    print()
    print("Press 'Y' if yes:")
    print("--------------------------------------------")
    pressed_key = input().upper().replace(" ", "")
    if pressed_key == "Y":
        print()
        load_data("SQLite")
    else:
        print("Terminated.")
else:
    if len(sys.argv) == 2:
        print("Not enough parameters.")
        sys.exit(1)

    elif len(sys.argv) == 3:
        param_name = sys.argv[1].upper().replace(" ", "")
        param_value = sys.argv[2].replace(" ", "")

        if param_name.upper() == "--server".upper() or param_name == "-s".upper():
            if param_value.upper() not in ["SQLite".upper(), "PostgreSQL".upper(), "SQLite+SQLAlchemy".upper()]:
                print()
                print(f"{param_value}: Unknown server type. Terminated.")
            else:
                print("--------------------------------------------")
                print("All data will be deleted and reloaded.")
                if param_value.upper() == "SQLite".upper():
                    print("Uploading will take up to 20 minutes.")
                else:
                    print("Uploading will take up to 100 minutes.")
                print()
                print("Press 'Y' if yes:")
                print("--------------------------------------------")

                pressed_key = input().upper().replace(" ", "")
                if pressed_key == "Y":
                    print()
                    load_data(param_value)
                else:
                    print("Terminated.")
        else:
            print("Error. Unknown parameter.")
            sys.exit(1)
    else:
        print("Too many parameters.")
        sys.exit(1)
