from datetime import timedelta
import datetime


initual_data_PERIOD_TYPES = \
        {
            "table_name": "PERIOD_TYPES", "find_and_update_or_insert": True, "id_column": "id", "donot_commit": False,
                "data":
                [   {"id": 0, "period_name": "Один тик", "phidden": 1},
                    {"id": 1, "period_name": "Одна минута", "phidden": 1},
                    {"id": 2, "period_name": "5 минут", "phidden": 1},
                    {"id": 3, "period_name": "10 минут", "phidden": 1},
                    {"id": 4, "period_name": "15 минут", "phidden": 1},
                    {"id": 5, "period_name": "30 минут", "phidden": 1},
                    {"id": 6, "period_name": "Час", "phidden": 0},
                    {"id": 7, "period_name": "День", "phidden": 1},
                    {"id": 8, "period_name": "Неделя", "phidden": 1},
                    {"id": 9, "period_name": "Месяц", "phidden": 1}
                ]
        }

initual_data_STOCKS = \
        {
            "table_name": "STOCKS", "find_and_update_or_insert": True, "id_column": "trade_kod", "donot_commit": False,
                "data":
                [   {"trade_kod": "GAZP", "mfd_id": 330, "full_name": 'ПАО "Газпром", акция обыкновенная', "short_name": "ГАЗПРОМ а/о", "order_field": 1, "phidden": 0},
                    {"trade_kod": "LKOH", "mfd_id": 632, "full_name": 'ПАО "Нефтяная компания "ЛУКОЙЛ", акция обыкновенная', "short_name": "ЛУКОЙЛ а/о", "order_field": 2, "phidden": 0},
                    {"trade_kod": "ROS", "mfd_id": 1373, "full_name": 'ПАО "Нефтяная компания "РОСНЕФТЬ", акция обыкновенная', "short_name": "РОСНЕФТЬ а/о", "order_field": 2, "phidden": 0},
                    {"trade_kod": "NLMK", "mfd_id": 913, "full_name": 'ПАО "Новолипецкий металлургический комбинат", акция обыкновенная', "short_name": "НЛМК", "order_field": 2, "phidden": 0},
                    {"trade_kod": "MMK", "mfd_id": 716, "full_name": 'ПАО "Магнитогорский металлургический комбинат", акция обыкновенная', "short_name": "ММК а/о", "order_field": 2, "phidden": 0}
                ]
        }

# Сгенерируем словарь с типами периодов:
PERIOD_TYPES = {
                initual_data_PERIOD_TYPES["data"][j]["period_name"]: initual_data_PERIOD_TYPES["data"][j]["id"]
                    for j in range(1, len(initual_data_PERIOD_TYPES["data"]))
               }

DEFAULT_PERIOD_TYPE = PERIOD_TYPES["Час"]

API_URL = "https://mfd.ru/export/handler.ashx/DataFile.txt?"
API_PARAMS = {
    "TickerGroup": "16",
    "Tickers": "330",
    "Alias": "False",
    "Period": "7",
    "timeframeValue": "1,",
    "timeframeDatePart": "day",
    "StartDate": "01.06.2019",
    "EndDate": "09.06.2019",
    "SaveFormat": "0",
    "SaveMode": "0",
    "FileName": "FileWithData.txt",
    "FieldSeparator": ";",
    "DecimalSeparator": ".",
    "DateFormat": "yyyyMMdd",
    "TimeFormat": "HHmmss",
    "DateFormatCustom": "",
    "TimeFormatCustom": "",
    "AddHeader": "true",
    "RecordFormat": "0",
    "Fill": "false"
}
API_PARAMS_DATE_FORMAT = "%d.%m.%Y"

DATE_FORMAT_SLASH = "%Y/%m/%d"
DATETIME_FORMAT_SLASH = "%Y/%m/%d %H:%M:%S"
DATETIME_FORMAT_DASH = "%Y-%m-%d %H:%M:%S"
DATETIME_FORMAT_IN_DB = DATETIME_FORMAT_DASH

FIRST_DAY_OF_PREDICTION_DATASET = (datetime.date.today() - datetime.timedelta(days=170)).strftime(DATE_FORMAT_SLASH)
FIRST_DAY_OF_PLOT = (datetime.date.today() - datetime.timedelta(days=7)).strftime(DATE_FORMAT_SLASH)

FIRST_DAY_IN_HISTORY = FIRST_DAY_OF_PREDICTION_DATASET
FIRST_DATETIME_IN_HISTORY = FIRST_DAY_OF_PREDICTION_DATASET + " 9:00:00"

MAX_LAG_FOR_PREDICTION = 9*100
DEPTH_OF_PREDICTION = 9*2 # Два дня
PREDICTION_STEP = timedelta(hours=1)
NEXT_DAY_AFTER_PREDICTION = datetime.datetime.today()+timedelta(hours=DEPTH_OF_PREDICTION+1)

MODULES_PARAM_GROUPS = {
    "initdb": ["db", "constants", "utils"],
    "start": ["db", "constants", "utils", "prediction", "inet"],
    "update": ["db", "constants", "utils", "inet"],
    "predict": ["db", "constants", "utils", "prediction", "inet"]
}
