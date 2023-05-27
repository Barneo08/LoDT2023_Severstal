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
        if events['M1']['dt']:
            exg['score'] = 1
        elif events['M3']['dt']:
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

    title = [name.split(f'А/М №{ex_num}_')[1] for name in tp_list['Names'] if f'А/М №{ex_num}' in name]
    return render_template("home/xgauster.html", title=title, exg_list=exg_list)






@app.route('/xgauster_data', methods = ['POST'])
def xgausterJSON():
        dh = DataHandler()

        n = request.json['id']
        d1 = parse(request.json['d1'])
        d2 = parse(request.json['d2'])
        time = pd.date_range(d1, d2, freq="1min")
        data ={}
        if n:
            events = dh.get_exh_events(n, d1, d2)

            m1 = pd.Series(events['M1']['dt']).dt.floor("T").to_list()
            m3 = pd.Series(events['M3']['dt']).dt.floor("T").to_list()
            df = pd.DataFrame(time, columns=['dt'])
            df['dt'] = df['dt'].dt.floor("T")
            df['M1'] = df['dt'].apply(lambda x: 1 if x in m1 else 0)
            df['M3'] = df['dt'].apply(lambda x: 1 if x in m3 else 0)
            print(df)
        #     data1 = df[[0,'M1','size1']] \
        #                 .rename({0:'x','M1':'y','size1':'r'}, axis=1) \
        #                 .to_json(orient='records')
        #     data3 = df[[0,'M3','size3']] \
        #                 .rename({0:'x','M3':'y','size3':'r'}, axis=1) \
        #                 .to_json(orient='records')
        #     # print(data1)
            
        # data = {
        #     "m1": data1,
        #     "m3": data3
        # }
        print(data)
        return jsonify(data)
