import os.path

import pandas as pd
from sklearn.linear_model import LogisticRegression
import joblib
import warnings

import src.constants as constants
import src.db as db
import src.utils as utils


warnings.filterwarnings('ignore')


def make_prediction_for_one_teh_place(X, teh_place_id):
    model_path = utils.get_get_model_full_file_path(teh_place_id, "M1")
    if os.path.isfile(model_path):
        model = joblib.load(model_path)
        prediction1 = model.predict_proba(X)[:, 1]
    else:
        prediction1 = None

    model_path = utils.get_get_model_full_file_path(teh_place_id, "M3")
    if os.path.isfile(model_path):
        model = joblib.load(model_path)
        prediction3 = model.predict_proba(X)[:, 1]
    else:
        prediction3 = None

    if prediction1 is None and prediction3 is None:
        return None
    if prediction1 is not None and prediction3 is None:
        return (prediction1 >= 0.5) * 1
    if prediction1 is None and prediction3 is not None:
        return (prediction3 >= 0.5) * 1

    prediction_ret = (prediction1 >= 0.5) * 1 + ((prediction3 > 0.5) & (prediction1 < 0.5)) * 2
    # prediction_ret = prediction_ret.astype("int")

    return prediction_ret


def make_prediction_for_one_exh(exh_id):
    X, y_full = prepare_dataset(exh_id=exh_id, process_type="test")
    for one_teh_place in y_full.columns:
        y_for_one_teh_place = make_prediction_for_one_teh_place(X, teh_place_id=one_teh_place)
        if y_for_one_teh_place is not None:
            y_full[one_teh_place] = y_for_one_teh_place

    dh = db.DataHandler("Pandas", keep_silence=True)
    y_exh_test_file_path = dh.connector.get_file_path(exh_id, type_data="Y", type_for="test")
    dh.connector.to_file(df=y_full, table_name=y_exh_test_file_path)


def make_prediction_for_all_exh():
    print()
    utils.log_print("Начато формирование предсказаний по каждому объекту", module_name="make_prediction_for_all_exh", color="b")
    for one_exh in constants.E_DICT.values():
        print(f" Эксгаузер: {one_exh}")
        make_prediction_for_one_exh(one_exh)
    utils.log_print("Предсказания сформированы", module_name="make_prediction_for_all_exh", color="b")
    print()


def prepare_models_for_one_exh(exh_id):
    X, y_full = prepare_dataset(exh_id=exh_id, process_type="train")
    # Первый уровень цикла - это технические места,
    # которым соответствует отдельный столбец в Y
    for one_teh_place in constants.Y_LIST.values():
        if one_teh_place[:len(exh_id)] != exh_id:
            continue
        # На всякий случай проверим есть ли такой столбец в списке столбцов в Y
        if one_teh_place in y_full.columns:
            # -----------------------------
            # Формирование модели для M1
            y_m1 = y_full[[one_teh_place]]
            y_m1[one_teh_place] = y_m1[one_teh_place].apply(lambda data: 1 if data == 1 else 0)
            unique_y_m1 = y_m1[one_teh_place].unique()
            if len(unique_y_m1) == 2:
                lr_m1 = LogisticRegression()
                lr_m1.fit(X, y_m1)
                model_file_name = utils.get_get_model_full_file_path(teh_place_id=one_teh_place, model_type="M1")
                if os.path.isfile(model_file_name):
                    os.remove(model_file_name)
                joblib.dump(lr_m1, model_file_name)
            # -----------------------------
            # Формирование модели для M3
            y_m3 = y_full[[one_teh_place]]
            y_m3[one_teh_place] = y_m3[one_teh_place].apply(lambda data: 2 if data == 2 else 0)
            unique_y_m3 = y_m3[one_teh_place].unique()
            if len(unique_y_m3) == 2:
                lr_m3 = LogisticRegression()
                lr_m3.fit(X, y_m3)
                model_file_name = utils.get_get_model_full_file_path(teh_place_id=one_teh_place, model_type="M3")
                if os.path.isfile(model_file_name):
                    os.remove(model_file_name)
                joblib.dump(lr_m3, model_file_name)
            # -----------------------------


def build_and_save_all_models():
    # Переберём все эксгаустеры и создадим
    # для каждого необходимый набор моделей:
    print()
    utils.log_print("Начата генерация моделей для всех объектов", module_name="build_and_save_all_models", color="b")
    for one_exh in constants.E_DICT.values():
        print(f" Эксгаузер: {one_exh}")
        prepare_models_for_one_exh(one_exh)
    utils.log_print("Генерация завершена", module_name="make_prediction_for_all_exh", color="b")
    print()


def prepare_dataset(exh_id, process_type="train"):
    dh = db.DataHandler("Pandas", keep_silence=True)
    x_file_path = dh.connector.get_file_path(exh_id, type_data="X", type_for=process_type)
    y_file_path = dh.connector.get_file_path(exh_id, type_data="Y", type_for=process_type)

    # Загружаем необходимый набор данных.
    df_x = None
    df_y = None
    if os.path.isfile(x_file_path):
        df_x = pd.read_parquet(x_file_path, engine="fastparquet")
    if os.path.isfile(y_file_path):
        df_y = pd.read_parquet(y_file_path, engine="fastparquet")

    if process_type == "train":
        # Эти данные необходимо привести к одному количеству строк,
        # так как на момент загрузки по обеим таблицам был "сдвиг"
        df_merged = df_x.merge(df_y, how="inner", left_index=True, right_index=True)
        df_x = df_merged[df_x.columns]
        df_y = df_merged[df_y.columns]
    elif process_type == "test":
        # Создадим заготовку:
        df_y = df_x[[df_x.columns[0]]].copy()
        df_y.rename(columns={df_x.columns[0]: "TempColumn"}, inplace=True)
        df_y["TempColumn"] = 0

        # Выберем все технические места для данного exh_id
        this_exh_all_teh_place = sorted([col for col in constants.Y_LIST.values() if col[:len(exh_id)] == exh_id])

        # В созданной заготовке создадим столбцы для каждого технического с нулями:
        for one_tp in this_exh_all_teh_place:
            df_y[one_tp] = df_y["TempColumn"]

        df_y.drop(["TempColumn"], axis=1, inplace=True)

    return df_x, df_y
