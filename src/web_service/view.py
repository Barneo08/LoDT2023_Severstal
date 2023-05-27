from datetime import datetime
import json

from flask import make_response, render_template
from flask import request
from flask import jsonify
import pandas as pd

from src.web_service.web_app import app
from src.db import DataHandler


# @app.route("/")
# def index():
#     title = "Стартовая страница"
#     content_title = "content_title."
#     content = "Здесь должен быть контент. Вот список:"

#     stocks_list = [["First", "1111", "Иванов"], ["Second", "2222", "Петров"], ["Third", "3333", "Сидоров"]]
#     my_list = []
#     for one_stock in stocks_list:
#         my_list = my_list + [["predicts/frame_" + one_stock[0], one_stock[2]]]

#     return render_template("index.html", title=title, content_title=content_title, content=content, html_list=my_list)

@app.route("/")
def index():
    dh = DataHandler()
    exg_list = dh.get_exh_list()

    return render_template("home/index.html", exg_list=exg_list)

# @app.route('/xgauster/<index>')
# def xgauster():
#     return render_template("home/xgauster.html")

@app.route('/xgauster/<id>')
def xgauster(id):
    dh = DataHandler()

    exg_list = dh.get_exh_list() # для меню

    d2= datetime.now()
    d1= d2 - pd.Timedelta(hours=3)

    tp_list = dh.get_exh_tp_list(id)

    # time = pd.date_range(d1, d2, freq="1min")

    data = []
    ex_num = id[1:]

    title = [name.split(f'А/М №{ex_num}_')[1] for name in tp_list['Names'] if f'А/М №{ex_num}' in name]
    # print(title)


    # for i in range(len(tp_list['IDs'])):
    #     id = tp_list['IDs'][i]
    #     events = dh.get_exh_events(id, d1, d2)
    #     # print('*'*15)

    #     m1 = pd.Series(events['M1']['dt']).dt.floor("T").to_list()
    #     m3 = pd.Series(events['M3']['dt']).dt.floor("T").to_list()
        # print(m1)
        # df = pd.DataFrame(time)
        # df[0] = df[0].dt.floor("T")
        # df['M1'] = df[0].apply(lambda x: 2 if x in m1 else 0)
        # df['M3'] = df[0].apply(lambda x: 1 if x in m3 else 0)
        # # print(df['M3'].to_list())
        # data.append({'title':title, 'id': id, 'm1':df['M1'].to_list(), 'm3':df['M3'].to_list()})
    return render_template("home/xgauster.html", title=title, exg_list=exg_list)






@app.route('/xgauster', methods = ['GET'])
def xgausterJSON():
        dh = DataHandler()
        n = request.args.get('key', '')
        if n:
             events = dh.get_exh_events(n, d1, d2)
        data = {
            "dt": [
                '2019-02-08 9:06:10',
                '2019-02-08 9:06:20',
                '2019-02-08 9:06:30',
                '2019-02-08 9:06:40',
                '2019-02-08 9:06:50',
                '2019-02-08 9:07:00',
                '2019-02-08 9:07:10',
                '2019-02-08 9:07:20',
                '2019-02-08 9:07:30',
                '2019-02-08 9:07:40',
                '2019-02-08 9:07:50',
                '2019-02-08 9:08:00',
                ],
            "tR1": [
                394.548,
                394.5481,
                394.5482,
                394.5483,
                394.5484,
                394.5485,
                394.5486,
                394.5487,
                394.5488,
                394.5489,
                394.549,
                394.5491,
            ],
            "tR2":[
                267.548,
                267.5481,
                267.5482,
                267.5483,
                267.5484,
                267.5485,
                267.5486,
                267.5487,
                267.5488,
                267.5489,
                267.549,
                267.5491,
            ]
        }

        return jsonify(data)
