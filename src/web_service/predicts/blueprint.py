import datetime
from flask import Blueprint
from flask import make_response
from flask import render_template
import io
import matplotlib.pyplot as plt
import pandas as pd

import src.constants as constants
import src.db as db


predicts = Blueprint("predicts", __name__, template_folder="templates")

if "db_connection" not in locals():
    db_connection = db.Data_handler()

@predicts.route("/")
def index():
    return(render_template("predicts/index.html"))

@predicts.route("/<inline_kod>")
def predict_detail(inline_kod):
    if inline_kod[:5]=="code_":
        trade_kod = inline_kod[5:]
        stocks_list = db_connection.get_stocks_list()
        for one_stock in stocks_list:
            if one_stock[0]==trade_kod:
                stoks_full_name = one_stock[2]
                stoks_short_name = one_stock[3]
                break
        my_list = [[f"plot_code_{trade_kod}_price_OPEN", "Цена открытия"]]
        my_list = my_list + [[f"plot_code_{trade_kod}_price_MIN", "Минимальная цена"]]
        my_list = my_list + [[f"plot_code_{trade_kod}_price_MAX", "Максимальная цена"]]
        my_list = my_list + [[f"plot_code_{trade_kod}_price_CLOSE", "Цена закрытия"]]

        return(render_template("predicts/description_page.html", html_list=my_list, trade_kod=trade_kod, stoks_full_name=stoks_full_name, stoks_short_name=stoks_short_name))
    elif inline_kod[:6]=="frame_":
        trade_kod = inline_kod[6:]
        return(render_template("predicts/frameset.html", trade_kod=trade_kod))
    elif inline_kod[:5]=="plot_":
        inline_kod = inline_kod[10:].split("_price_")
        trade_kod = inline_kod[0]
        price_type = inline_kod[1]
        return(render_template("predicts/plot_page.html", trade_kod=trade_kod, price_type=price_type))
    elif inline_kod[:5]=="pict_":
        inline_kod = inline_kod[10:].split("_price_")
        trade_kod = inline_kod[0]
        price_type = inline_kod[1].upper()
        
        mfd_id = get_id_for_code(trade_kod)
        df = db_connection.get_stocks_prices_pd(mfd_id=mfd_id, id_period_type=constants.DEFAULT_PERIOD_TYPE, price_type=price_type, dt_begin=constants.FIRST_DAY_OF_PLOT, ppredict=0)
#        df.reset_index(drop=True)
        df2 = db_connection.get_stocks_prices_pd(mfd_id=mfd_id, id_period_type=constants.DEFAULT_PERIOD_TYPE, price_type=price_type, dt_begin=constants.FIRST_DAY_OF_PLOT, dt_end=constants.NEXT_DAY_AFTER_PREDICTION, ppredict=1)
#        df2.reset_index(drop=True)
        df2.index = [x for x in range(df.shape[0], df2.shape[0] + df.shape[0])]
        df3 = pd.DataFrame([df.tail(1).price.values[0], df2.head(1).price.values[0]])
#        df3.reset_index(drop=True)
        df3.index = [df.shape[0] - 1, df.shape[0]]

        fig, ax = plt.subplots(figsize=(12, 4))

        all_plot_points_count = df2.shape[0] + df.shape[0]
        show_grid_plot_points = int(all_plot_points_count / 8)

        xx = [point for point in range(0, all_plot_points_count, show_grid_plot_points)]
        xx.append(df.shape[0]-1)

        xlabels = []
        dfnew = df.append(df2)
        for i in xx:
            curr_date = datetime.datetime.strptime(dfnew.price_dt.values[i], constants.DATETIME_FORMAT_DASH)
            day = str(curr_date.day).zfill(2)
            month = str(curr_date.month).zfill(2)
            hour = str(curr_date.hour).zfill(2)
            minute = str(curr_date.minute).zfill(2)
            xlabels.append(f'{day}/{month} {hour}:{minute}')
        ax.set_xticks(xx)
        ax.set_xticklabels(xlabels, rotation=330, fontsize=7)


        ax.plot(df["price"], "b", label="Фактические данные")
        ax.plot(df2["price"], "r", label="Предсказание")
        ax.plot(df3, "gray")

        plt.legend(loc="best")
        ax.grid(True)

        plt.title("График изменения стоимости акции")

        img = io.BytesIO()
        plt.savefig(img, format='png')

        response = make_response(img.getvalue())
        response.headers['Content-Type'] = 'img/png'
        response.headers['Content-Length'] = len(img.getvalue())
        response.headers['Content-Disposition'] = 'inline; filename="%s"' % (inline_kod)
        img.close()

        return(response)

def get_id_for_code(trade_kod):
    stocks_list = db_connection.get_stocks_list()
    for one_stock in stocks_list:
        if one_stock[0]==trade_kod:
            mfd_id = one_stock[1]
            break
    return(mfd_id)


