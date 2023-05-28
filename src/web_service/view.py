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
    exg_list = dh.get_exh_list()
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

    for exg in exg_list:
        events = dh.get_exh_events(exg['id'], d1, d2) 
        if events['M1_TEST'][0]:
            exg['score'] = 1
        elif events['M3_TEST'][0]:
            exg['score'] = 2
        else:
            exg['score'] = 0
    
    return render_template("home/index.html", exg_list=exg_list, date={'d1':d1, 'd2':d2})

# @app.route('/xgauster/<index>')
# def xgauster():
#     return render_template("home/xgauster.html")

@app.route('/xgauster/<id>', methods=['GET'])
def xgauster(id):
    dh = DataHandler()
    d1 = request.args.get('b')
    d2 = request.args.get('e')
    print(d1, d2)
    exg_list = dh.get_exh_list() # для меню

    d2= datetime.now()
    d1= d2 - pd.Timedelta(hours=3)

    tp_list = dh.get_exh_tp_list(id)

    # time = pd.date_range(d1, d2, freq="1min")

    data = []
    ex_num = id[1:]

    title = [{'t':name, 'n':name.split(f'А/М №{ex_num}_')[1]} for name in tp_list['Names'] if f'А/М №{ex_num}' in name]
    return render_template("home/xgauster.html", title=title, exg_list=exg_list)






@app.route('/xgauster_data', methods = ['POST'])
def xgausterJSON():
        dh = DataHandler()

        n = request.json['id']
        tp = request.json['tp']
        tp_list = dh.get_exh_tp_list(n)
        for i in range(len(tp_list['Names'])):
            if tp_list['Names'][i] == tp:
                tp_id = tp_list['IDs'][i]
                break
        d1 = parse(request.json['d1'])
        d2 = parse(request.json['d2'])
        time = pd.date_range(d1, d2, freq="5min")
        data ={}
        if n:
            events = dh.get_exh_events(tp_id, d1, d2)
            m1 = pd.Series(events['M1_TEST'][0]).dt.floor("5min").to_list()
            m3 = pd.Series(events['M3_TEST'][0]).dt.floor("5min").to_list()
            m1tr = pd.Series(events['M1_TRAIN'][0]).dt.floor("5min").to_list()
            m3tr = pd.Series(events['M3_TRAIN'][0]).dt.floor("5min").to_list()
            df = pd.DataFrame(time, columns=['dt'])
            df['dt'] = df['dt'].dt.floor("5min")
            df['M1_TEST'] = df['dt'].apply(lambda x: 1 if x in m1 else 0)
            df['size1'] = df['M1_TEST'] * 10
            df['M3_TEST'] = df['dt'].apply(lambda x: 1 if x in m3 else 0)
            df['size3'] = df['M3_TEST'] * 10
            df['M1_TRAIN'] = df['dt'].apply(lambda x: 1.2 if x in m1tr else 0)
            df['size1tr'] = df['M1_TRAIN'] * 5
            df['M3_TRAIN'] = df['dt'].apply(lambda x: 1.2 if x in m3tr else 0)
            df['size3tr'] = df['M3_TRAIN'] * 5
            df['index'] = df.index
            data1 = df[['dt','M1_TEST','size1']] \
                        .rename({'dt':'x','M1_TEST':'y','size1':'r'}, axis=1) \
                        .to_json(orient='records').replace('"','')
            data3 = df[['dt','M3_TEST','size3']] \
                        .rename({'dt':'x','M3_TEST':'y','size3':'r'}, axis=1) \
                        .to_json(orient='records').replace('"','')
            data1tr = df[['dt','M1_TRAIN','size1tr']] \
                        .rename({'dt':'x','M1_TRAIN':'y','size1tr':'r'}, axis=1) \
                        .to_json(orient='records').replace('"','')
            data3tr = df[['dt','M3_TRAIN','size3tr']] \
                        .rename({'dt':'x','M3_TRAIN':'y','size3tr':'r'}, axis=1) \
                        .to_json(orient='records').replace('"','')
            dt = df['dt'].dt.strftime('%H:%M').to_json(orient="values")
            
        data = {
            "m1": data1,
            "m3": data3,
            "m1tr": data1tr,
            "m3tr": data3tr,
            'dt': dt
        }
        # print(data)
        return jsonify(data)
