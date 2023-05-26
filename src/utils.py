import datetime
import sys
import time
import logging

from src.config import *


def prepare_df(df):
    columns_list = df.columns

    if not ( 
                ("<DATE>" in columns_list) and
                ("<TIME>" in columns_list) and 
                ("<CLOSE>" in columns_list) and
                ("<VOL>" in columns_list)
            ):
        print("prepare_df: Указанный файл не содержит нужных столбцов.")
        return False

    if not ("<OPEN>" in columns_list): df["<OPEN>"] = df["<CLOSE>"]
    if not ("<HIGH>" in columns_list): df["<HIGH>"] = df["<CLOSE>"]
    if not ("<LOW>" in columns_list): df["<LOW>"] = df["<CLOSE>"]
    
    if not df.shape[0]==0:
        df["<DATE>"] = df["<DATE>"].astype(str)
        df["<TIME>"] = df["<TIME>"].apply(lambda x: '{0:06}'.format(x))
        df["<DT>"] = df[["<DATE>", "<TIME>"]].apply(lambda dt: datetime.datetime.strptime(dt[0]+" "+dt[1], "%Y%m%d %H%M%S"), axis=1)
    else:
        df["<DT>"] = df["<DATE>"]
    # Вернём только нужные столбцы и в нужном порядке.
    if not ("ppredict" in columns_list):
        df["ppredict"] = 0
    return df[["<DT>", "<OPEN>", "<HIGH>", "<LOW>", "<CLOSE>", "<VOL>", "ppredict"]]


class Timer:
    """
    Создаётся объект, который позволяет
    засекать время и отображать сколько
    времени прошло в читаемом формате.
    """
    def __init__(self, timer_name="", only_on_show=True):
        self.timer_name = timer_name
        self.start_position_of_timer = time.time()
        duration_in_seconds = int(time.time() - self.start_position_of_timer)
        if only_on_show:
            pass
        else:
            self.print_text(duration_in_seconds)

    def show(self):
        # Функция для отображения времени
        duration_in_seconds = int(time.time() - self.start_position_of_timer)
        self.print_text(duration_in_seconds)
        print()

    def print_text(self, duration_in_seconds):
        print_timer_str = "--- {0:0>2}:{1:0>2} ---".format(duration_in_seconds // 60, duration_in_seconds % 60)
        print_spaces = " " * 5
        print_separator_line = "-" * (len(self.timer_name) + len(print_spaces)) + "-" * len(print_timer_str)

        print_timer_str = self.timer_name + print_spaces + print_timer_str

        print(print_separator_line)
        print(print_timer_str)
        print(print_separator_line)


def d2dt(date):
    # Преобразование даты в ДатаВремя
    return datetime.datetime(date.year, date.month, date.day)


def last_second_of_date(date):
    # Получение последнего мгновения (секунды) в указанных сутках
    return d2dt(date) + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)


def if_module_exist(module_name):
    folder_module = os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), "src", module_name)  # Полный путь до модуля, если это папка.
    init_in_folder_module = os.path.join(folder_module, "__init__.py")  # Полный путь до __init__.py в папке.
    root_module = folder_module + ".py"  # Полный путь до модуля, если он лежит в корне.

    # Проверим их наличие:
    if not os.path.exists(root_module) and not (os.path.isdir(folder_module) and os.path.exists(init_in_folder_module)):
        print(f"There is no module or package '{module_name}'.")
        return False
    else:
        return True


def if_all_modules_exist(modules_list):
    # Пробежим по списку полученных имён модулей и проверим их существование.
    # Если хотя бы одного нет - вернём False.
    p_flag = True
    for one_module in modules_list:
        if not if_module_exist(one_module):
            p_flag = False
            break
    return p_flag


def log_print(text, module_name="NoModuleInfo", to_screen=True):
    """
    Упрощение логирования событий, при необходимости вывод на экран.
    """
    if to_screen:
        print(f"{module_name}:", text)

    if CONFIG.DEBUG:
        logging.debug(f"{module_name}: {text}")


def get_home_dir():
    return os.path.abspath(os.curdir)


def sep_digits(num, sep=" "):
    return '{0:,}'.format(num).replace(',', sep)