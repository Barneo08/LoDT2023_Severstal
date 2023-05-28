from datetime import datetime

import json
from dateutil.parser import parse

from flask import make_response, render_template
from flask import request
from flask import jsonify
import pandas as pd

from src.web_service.web_app import app
from src.db import DataHandler


@app.route("/", methods=['GET', 'POST'])
def index():
    dh = DataHandler()
    if request.method == 'POST':
        d1 = request.form.get('d1')  # запрос к данным формы
        d2 = request.form.get('d2')
        try:
            d2 = parse(d2)
            d1 = parse(d1)
        except:
            d2= datetime.now()
            d1= d2 - pd.Timedelta(hours=3)
    else:
        d2= datetime.now()
        d1= (d2 - pd.Timedelta(hours=3))

    exg_list = dh.get_exh_list(d1, d2)
    print(exg_list)
    
    return render_template("home/index.html", exg_list=exg_list, date={'d1':d1, 'd2':d2})


@app.route('/xgauster/<id>', methods=['GET'])
def xgauster(id):
    dh = DataHandler()
    d2 = request.args.get('e', datetime.now())
    d1 = request.args.get('b', datetime.now() - pd.Timedelta(hours=3))

    exg_list = dh.get_exh_list() # для меню

    tp_list = dh.get_exh_tp_list(id)

    ex_num = id[1:]

    print(tp_list)

    title = [{'id':id, 'name':name.split(f'А/М №{ex_num}_')[1]} for name, id in zip(tp_list['Names'],tp_list['IDs']) if f'А/М №{ex_num}' in name]
    return render_template("home/xgauster.html", title=title, exg_list=exg_list, date={'d1':d1, 'd2':d2})


@app.route('/xgauster_data', methods = ['POST'])
def xgausterJSON():
        dh = DataHandler()

        tp_id = request.json['tp']
        d1 = parse(request.json['d1'])
        d2 = parse(request.json['d2'])
        step = '5min'
        time = pd.date_range(d1, d2, freq=step)
        data ={}
        if tp_id:
            events = dh.get_exh_events(tp_id, d1, d2)

            m1 = pd.Series(events['M1_TEST']).dt.floor(step).to_list()
            m3 = pd.Series(events['M3_TEST']).dt.floor(step).to_list()
            m1tr = pd.Series(events['M1_TRAIN']).dt.floor(step).to_list()
            m3tr = pd.Series(events['M3_TRAIN']).dt.floor(step).to_list()

            df = pd.DataFrame(time, columns=['dt'])
            df['dt'] = df['dt'].dt.floor(step)
            df['M1_TEST'] = df['dt'].apply(lambda x: 1 if x in m1 else 0)
            df['size1'] = df['M1_TEST'] * 10
            df['M3_TEST'] = df['dt'].apply(lambda x: 1 if x in m3 else 0)
            df['size3'] = df['M3_TEST'] * 10
            df['M1_TRAIN'] = df['dt'].apply(lambda x: 1.2 if x in m1tr else 0)
            df['size1tr'] = df['M1_TRAIN'] * 5
            df['M3_TRAIN'] = df['dt'].apply(lambda x: 1.2 if x in m3tr else 0)
            df['size3tr'] = df['M3_TRAIN'] * 5
            df['index'] = df.index

            dt = df['dt'].dt.strftime('%H:%M').to_json(orient="values")
            data1 = [{'x':x.timestamp(),"y":y, "r":r} for x,y,r in zip(df['dt'],df['M1_TEST'],df['size1'])]
            data3 = [{'x':x.timestamp(),"y":y, "r":r} for x,y,r in zip(df['dt'],df['M3_TEST'],df['size3'])]
            data1tr = [{'x':x.timestamp(),"y":y, "r":r} for x,y,r in zip(df['dt'],df['M1_TRAIN'],df['size1tr'])]
            data3tr = [{'x':x.timestamp(),"y":y, "r":r} for x,y,r in zip(df['dt'],df['M3_TRAIN'],df['size3tr'])]
            data = {
                "m1": data1,
                "m3": data3,
                "m1tr": data1tr,
                "m3tr": data3tr,
                'dt': dt
            }
        return jsonify(data)
