import datetime
import pandas as pd
from sklearn.linear_model import LinearRegression
from tqdm import tqdm
import warnings

import src.constants as constants
import src.db as db
import src.utils as utils


warnings.filterwarnings('ignore')


def make_prediction():
    """
        Do prediction for each stock
    """
    otimer = utils.Timer("Get started process of stocs's prices prediction:")
    dh = db.Data_handler()
    stocks_list = dh.get_stocks_list()
    for one_stoks in stocks_list:
        print("\nPredicting prices for {}, {}".format(one_stoks[0], one_stoks[2]))
        make_stock_prediction(mfd_id=one_stoks[1], db_connection=dh)
    otimer.show()


def make_stock_prediction(mfd_id, db_connection):
    """
        Make pridiction for 4 prices.
    """
    X_tmp = make_stock_prediction_for_one_price(db_connection, mfd_id=mfd_id, sprice_type="OPEN")

    X_predicted = X_tmp[["y"]].copy()
    X_predicted["price_open"] = X_predicted["y"]
    X_predicted["vol"] = 0
    X_predicted["ppredict"] = 1

    X_tmp = make_stock_prediction_for_one_price(db_connection, mfd_id=mfd_id, sprice_type="CLOSE")
    X_predicted["price_close"] = X_tmp["y"]

    X_tmp = make_stock_prediction_for_one_price(db_connection, mfd_id=mfd_id, sprice_type="MIN")
    X_predicted["price_min"] = X_tmp["y"]

    X_tmp = make_stock_prediction_for_one_price(db_connection, mfd_id=mfd_id, sprice_type="MAX")
    X_predicted["price_max"] = X_tmp["y"]

    X_predicted = X_predicted.reset_index()
    X_predicted = X_predicted[["price_dt", "price_open", "price_min", "price_max", "price_close", "vol", "ppredict"]]

    db_connection.load_stock_prises_from_df2db(mfd_id=mfd_id, id_period_type=constants.DEFAULT_PERIOD_TYPE, df=X_predicted)


def make_stock_prediction_for_one_price(db_connection, mfd_id, sprice_type):
    """
        Do prediction for one element - one type of price.
    """
    print("Формируем предсказание для цены: ", sprice_type)
    df = db_connection.get_stocks_prices_pd(
                    mfd_id=mfd_id,
                    id_period_type=constants.DEFAULT_PERIOD_TYPE,
                    price_type=sprice_type,
                    dt_begin=constants.FIRST_DAY_OF_PREDICTION_DATASET,
                    ppredict=0
                                            )
    df["price_dt"] = df["price_dt"].apply(lambda x: datetime.datetime.strptime(x, constants.DATETIME_FORMAT_DASH))
    # Максимальная дата - дата последней строки:
    last_datetime = df.price_dt.max()
    df_last_row = df.shape[0]
    # Подготовим строчки для предсказания, добавив их в основную таблицу - 
    # тут они правильно заполнятся:
    for j in range(constants.DEPTH_OF_PREDICTION):
        last_datetime = last_datetime + constants.PREDICTION_STEP
        if last_datetime.hour>=20:
            last_datetime = last_datetime + datetime.timedelta(hours=(9+24-last_datetime.hour))

        new_row = pd.DataFrame([{"price": 0, "price_dt": last_datetime, "vol": 0}])
        new_row.index = [df.shape[0]]
        df = df.append(new_row)

    X_train, X_test = prepare_dataset(df, df_last_row)

    X_test = prepare_prediction(X_train, X_test)
    return X_test


def prepare_prediction(X, X_test):
    """
        It's prepare some columns with information of
        previous price and then make prediction using LinearRegression.
    """
    ndivider = X.shape[0]
    for num_row in tqdm(range(X_test.shape[0])):
        # Получим данные для обучения:
        X_train = X.drop(["y"], axis=1)
        y_train = X["y"]
        
        # Сформируем строку, на которой будем делать предсказание
        X_row4predict = X_test[num_row:(num_row + 1)].copy()
        # Скопируем из Х последнюю строку:
        X_last_row = X.tail(1).copy()
        # Сдвинем в тестовой строке все лаги на 1, заменив первый на целевой признак из обучающей выборки:
        for j in range(constants.MAX_LAG_FOR_PREDICTION, 1, -1):
            X_row4predict["lag_{}".format(j)] = X_last_row["lag_{}".format(j-1)].values[0]

        # А первый лаг - это целевой признак:
        X_row4predict["lag_1"] = X_last_row["y"].values[0]
        # Обучим:
        lr = LinearRegression()
        lr.fit(X_train, y_train)
        # Сделаем предсказание, убрав столбец с целевым признаком:
        prediction = lr.predict(X_row4predict.drop(["y"], axis=1))
        X_row4predict["y"] = prediction[0]

        # Добавим строку с предсказанием в обучающйю выборку для дальнейшего использования:
        # X_row4predict.index = [X.shape[0]]
        X = X.append(X_row4predict)
        X["y"] = X["y"]

    return X[ndivider:]


def prepare_dataset(df, ndivider):
    lag_end = constants.MAX_LAG_FOR_PREDICTION

    dataset = df.copy()
    dataset = dataset.set_index("price_dt")
    dataset = dataset[["price"]]
    dataset.columns = ["y"]

    # добавляем лаги исходного ряда в качестве признаков
    for j in range(1, lag_end + 1):
        dataset["lag_{}".format(j)] = dataset.y.shift(j)

    dataset["month"] = dataset.index.month
    dataset["day"] = dataset.index.day
    dataset["hour"] = dataset.index.hour
    dataset["minute"] = dataset.index.minute
    dataset["weekday"] = dataset.index.weekday + 1
    for j in range(1,13):
        dataset[f"month{j}"] = dataset["month"].apply(lambda x: 1 if x == j else 0)
    for j in range(1,32):
        dataset[f"day{j}"] = dataset["day"].apply(lambda x: 1 if x == j else 0)
    for j in range(1,8):
        dataset[f"weekday{j}"] = dataset["weekday"].apply(lambda x: 1 if x == j else 0)
    for j in range(8,21):
        dataset[f"hour{j}"] = dataset["hour"].apply(lambda x: 1 if x == j else 0)
    for j in [0,10,20,30,40,50]:
        dataset[f"minute{j}"] = dataset["minute"].apply(lambda x: 1 if x == j else 0)

    dataset["hour"] = dataset.index.hour
    dataset['is_weekend'] = dataset.weekday.isin([5,6])*1
    # выкидываем признаки, созданные для кодирования средних
    dataset.drop(["month", "day", "weekday", "hour", "minute"], axis=1, inplace=True)

    X_train = dataset[:ndivider]
    X_test = dataset[ndivider:]
    # Удалим строки с образовавшимися при смещении пустыми ячейками:
    X_train = X_train.dropna()
    return X_train, X_test
