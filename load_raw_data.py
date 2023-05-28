import sys
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

from src.config import *
import src.constants as constants
import src.utils as utils
from src.utils import p
import src.db as db

import warnings


def load_data(sql_server_type):
    module_name = "Load raw data to DB"

    if not CONFIG.RAW_FILES_UPLOAD_ENABLED:
        print("----------------------------------------------------------------------")
        utils.log_print("Загрузка 'сырых' файлов заблокирована в концигурационном файле", module_name=module_name, color="r")
        print("----------------------------------------------------------------------")
        print()
        print("Terminated.")
        return

    utils.log_print(f"Начата загрузка 'сырых' данных в структуру ({sql_server_type})...", module_name=module_name)
    if not utils.if_all_modules_exist(constants.MODULES_PARAM_GROUPS["initdb"]):
        utils.log_print("Один из модулей программы отсутствует. Работа прекращена.", module_name="Load raw data to DB")
    else:
        # Создаём объект для работы с БД
        dh = db.DataHandler(sql_server_type, keep_silence=False)
        print()

        conf = {
            "Y_TRAIN": {"raw_data_file": CONFIG.Y_TRAIN_FILE, "columns_subs": constants.Y_LIST, "type": "prediction", "task": "unconverted", "nan_enabled": False},
            "Y_TRAIN_GROUPED": {"raw_data_file": CONFIG.Y_TRAIN_FILE, "columns_subs": constants.Y_LIST, "type": "prediction", "task": "group", "nan_enabled": False},
            "X_TRAIN_GROUPED": {"raw_data_file": CONFIG.X_TRAIN_FILE, "columns_subs": constants.SENSOR_FIELD_NAMES_LIST, "type": "train", "task": "group", "nan_enabled": True},
            "X_TEST_GROUPED": {"raw_data_file": CONFIG.X_TEST_FILE, "columns_subs": constants.SENSOR_FIELD_NAMES_LIST, "type": "test", "task": "group", "nan_enabled": True},
        }

        for raw_table_name in conf.keys():
            # Создаём таймер
            timer = utils.Timer(f"Таймер загрузки: {p(color='b', bold=1)}{raw_table_name}{p()}", only_on_show=False)

            # Получаем информация о загружаемой таблице
            # В нашем случае данные хранятся в файлах parquet
            file_path = os.path.join(os.path.abspath(os.curdir), CONFIG.RAW_DATA_FULL_PATH, conf[raw_table_name]["raw_data_file"])
            df = pd.read_parquet(file_path, engine="fastparquet")

            # Заменим названия столбцов на удобные из CONFIG:
            df.rename(columns=conf[raw_table_name]["columns_subs"], inplace=True)

            rows = df.shape[0]
            print(f"Количество строк для загрузки: {utils.sep_digits(rows)} шт.")
            flag_for_first_print = True

            for element in constants.E_LIST_ID:
                # Получим префикс (имя) для эксгаустера
                e_name = element + constants.EXH_SEPARATOR
                # Отберём колонки в данных, которые начинаются с этого имени
                y_columns_list = sorted([one_column for one_column in df.columns if one_column[:len(e_name)] == e_name])
                y_columns_list = sorted(y_columns_list)
                if len(y_columns_list) != 0:
                    df_2_load = df[y_columns_list]

                    if conf[raw_table_name]["task"] == "group":
                        if flag_for_first_print:
                            min_gr = utils.sep_digits(CONFIG.MINUTES_ROWS_IN_GROUP)
                            imm_depth = utils.sep_digits(CONFIG.ROWS_IMMERSION_DEPTH)
                            imm_depth_in_min = utils.sep_digits(CONFIG.ROWS_IMMERSION_DEPTH * CONFIG.MINUTES_ROWS_IN_GROUP)
                            print(f"Строки группируются по {min_gr} мин.")
                            print(f"Глубина истории {imm_depth} записей, т.е. в сумме {imm_depth_in_min} мин.")
                        # -----------------------------------------------
                        df_2_load = preparation_mean_and_group(
                            df=df_2_load,
                            is_prediction=(conf[raw_table_name]["type"] == "prediction"),
                            nan_enabled=conf[raw_table_name]["nan_enabled"],
                            text=element,
                        )
                        # -----------------------------------------------
                        if flag_for_first_print:
                            flag_for_first_print = False
                            print(f"Данные сгруппированы. Количество строк в итоговой таблице: {utils.sep_digits(df_2_load.shape[0])} шт.")
                            if dh.sql_server != "Pandas":
                                print(f"Загрузка осуществляется блоками по {utils.sep_digits(CONFIG.UPLOAD_ROWS)} строк.")
                    elif conf[raw_table_name]["task"] == "unconverted" and conf[raw_table_name]["task"] == "prediction":
                        pass


                    if flag_for_first_print:
                        flag_for_first_print = False
                        if dh.sql_server != "Pandas":
                            print(f"Загрузка осуществляется блоками по {utils.sep_digits(CONFIG.UPLOAD_ROWS)} строк.")

                    # Имя для таблицы:
                    table_name = e_name + raw_table_name
                    # Загружаем данные
                    utils.log_print(f"Загрузка сырых данных y_train для эксгаузера {element} в DBase в таблицу {table_name}.", module_name=module_name, to_screen=False)

                    # Если таблица есть, то её надо удалить.
                    dh.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    dh.commit()

                    if dh.sql_server == "Pandas":
                        dh.connector.to_file(df_2_load, table_name)
                    else:
                        # Загрузим данные частями по UPLOAD_ROWS
                        def data_2_sql(num1, num2):
                            df_2_load.iloc[num1: num2].to_sql(table_name, con=dh.connector, if_exists="append")

                        for num in tqdm(range(int(rows / CONFIG.UPLOAD_ROWS) + 1), ncols=75, desc=f"Эксгаузер: {element}"):
                            data_2_sql(num * CONFIG.UPLOAD_ROWS, (num * CONFIG.UPLOAD_ROWS) + CONFIG.UPLOAD_ROWS)

            # Выведем информацию о времени исполнения блока кода:
            timer.show()


def preparation_mean_and_group(df, is_prediction, nan_enabled, text=""):
    # Здесь проставим признаки для NaN
    # Добавим дубли столбцов, которые показывают есть или нет NaN (0 - есть, 1 - нет)
    warnings.filterwarnings("ignore")
    if CONFIG.CREATE_NAN_FLAGS and nan_enabled:
        for one_col in df.columns:
            df[one_col + constants.NOT_NAN_SUFFIX] = df[[one_col]].isna() * (-1) + 1

    # Заполним NaN скользящим средним, а те, которые не заполнятся, то их - средним
    df.fillna(df.rolling(CONFIG.MINUTES_ROWS_IN_GROUP * 60, min_periods=1).mean(), inplace=True)
    df.fillna(df.mean(), inplace=True)
    df.fillna(0, inplace=True)

    # Проставим новое время, по которому потом сгруппируем в одно строку
    df = df.reset_index()

    # Создадим основу для группировки - уберём секунды и лишние минуты:
    if is_prediction:
        # Для обучения модели необходимо сместить имеющееся предсказание в прошлое на необходимое время:
        fh_hours = CONFIG.HOURS_FORECAST_HORIZON
        fh_minutes = CONFIG.MINUTES_FORECAST_HORIZON
    else:
        fh_hours = 0
        fh_minutes = 0

    df["DT"] = df["DT"].apply(
        lambda x: x - timedelta(hours=fh_hours, minutes=fh_minutes, seconds=x.second) - timedelta(
            minutes=(x.minute - ((x.minute // CONFIG.MINUTES_ROWS_IN_GROUP) * CONFIG.MINUTES_ROWS_IN_GROUP))
            )
    )

    if is_prediction:
        # Это файл с тренировочными предсказаниями.
        # Для начала его надо сместить на время указанное в CONFIG.
        df = df.reset_index()
        df["DT"] = df["DT"].apply(lambda x: x - timedelta(hours=CONFIG.HOURS_FORECAST_HORIZON, minutes=CONFIG.MINUTES_FORECAST_HORIZON))
        df = df.set_index("DT")

        # Его группировать с выделением среднего нельзя.
        # На группируемом диапазоне него должны остаться значение:
        #  - М1 если оно было хотя бы раз;
        #  - М3, если оно было хотя бы раз и не было М1;
        #  - в противном случае.
        for column_name in df.columns:
            df.loc[df[column_name] == 1, column_name] = 10
        df = df.groupby('DT').max()
        for column_name in df.columns:
            df.loc[df[column_name] == 10, column_name] = 1
    else:
        # Это файл с событиями и их мы усредняем:
        df = df.groupby('DT').mean()

        # Добавим справа столбцы из истории с глубиной CONFIG.MINUTES_ROWS_IN_GROUP.
        df_hist = df.copy()
        for step_num in tqdm(range(1, CONFIG.ROWS_IMMERSION_DEPTH + 1), ncols=75, desc=f"{text}: Добавление истории"):
            df_hist = df_hist.reset_index()
            df_hist["DT"] = df_hist["DT"].apply(lambda x: x + timedelta(minutes=CONFIG.MINUTES_ROWS_IN_GROUP))
            df_hist = df_hist.set_index("DT")

            df = df.merge(df_hist, left_index=True, right_index=True, suffixes=("", f"__L{step_num}"))

    if "index" in df.columns:
        df.drop(["index"], axis=1, inplace=True)
    warnings.filterwarnings("default")

    return df


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("--------------------------------------------")
        print(f"Вы действительно хотите загрузить 'сырые' данные")
        print(f"в базу данных для последующей обработки и")
        print(f"формирования моделей и прогнозов по техместам?")
        print()
        print(f"По умолчанию данные будут загружены в {p(color='b', bold=1)}Pandas{p()}.")
        print(f"Вы можете использовать другую структуру хранения,")
        print(f"указав при запуски один из следующих параметров:")
        print(f"--server SQLite")
        print(f"--server PostgreSQL")
        print()
        print(f"Наберите на клавиатуре '{p(color='r', bold=1)}Да{p()}'")
        print(f"и подтвердите действие, нажав Enter:")
        print("--------------------------------------------")
        pressed_key = input().upper().replace(" ", "")
        if pressed_key == "ДА":
            common_timer = utils.Timer(f"Общая длительность произведённой подготовки:", only_on_show=True)
            print()
            load_data("Pandas")
            import src.prediction as prediction

            timer = utils.Timer(f"Длительность формирования моделей:", only_on_show=True)
            prediction.build_and_save_all_models()
            timer.show()
            print()
            timer = utils.Timer(f"Длительность предсказаний:", only_on_show=True)
            prediction.make_prediction_for_all_exh()
            timer.show()
            print()
            common_timer.show()
        else:
            print("Завершение работы.")
    else:
        if len(sys.argv) == 2:
            print("Не достаточный список параметров.")
            sys.exit(1)

        elif len(sys.argv) == 3:
            param_name = sys.argv[1].upper().replace(" ", "")
            param_value = sys.argv[2].replace(" ", "")

            if param_name.upper() == "--server".upper() or param_name == "-s".upper():
                print("Функциональность заблокирована.")
                print("Завершение работы.")
                if param_value.upper() not in ["Pandas".upper(), "SQLite".upper(), "PostgreSQL".upper(), "SQLite+SQLAlchemy".upper()]:
                    print()
                    print(f"{param_value}: Unknown server type. Terminated.")
                else:
                    print("--------------------------------------------")
                    print("Все имеющиеся данные будут удалены и перезаписаны.")
                    if param_value.upper() == "Pandas".upper():
                        print("Загрузка займёт больше 1 часа.")
                    print()
                    print(f"Наберите на клавиатуре '{p(color='r', bold=1)}Да{p()}'")
                    print(f"и подтвердите действие, нажав Enter:")
                    print("--------------------------------------------")

                    pressed_key = input().upper().replace(" ", "")
                    if pressed_key == "ДА":
                        print()
                        load_data(param_value)
                    else:
                        print("Завершение работы.")
            else:
                print("Ошибка. Неизвестный параметр.")
                sys.exit(1)
        else:
            print("Слишком много параметров.")
            sys.exit(1)
