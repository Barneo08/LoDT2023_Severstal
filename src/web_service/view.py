import json

from flask import make_response, render_template
from flask import request
from flask import jsonify

from src.web_service.web_app import app


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
    exg_list = [{'id':4, 'title': 'Агломашина №4', 'score': 0},
                {'id':5, 'title': 'Агломашина №5', 'score': 0},
                {'id':6, 'title': 'Агломашина №6', 'score': 1},
                {'id':7, 'title': 'Агломашина №7', 'score': 0},
                {'id':8, 'title': 'Агломашина №8', 'score': 2},
                {'id':9, 'title': 'Агломашина №9', 'score': 0}]

    return render_template("home/index.html", exg_list=exg_list)

@app.route('/xgauster/<int:index>')
def xgauster():
    return render_template("home/xgauster.html")

@app.route('/xgauster', methods = ['GET'])
def xgausterJSON():
        n = request.args.get('key', '')
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

        # response = app.response_class(
        #     response=json.dumps(data),
        #     status=200,
        #     mimetype='application/json'
        # )

        # return make_response(jsonify(data), 200)

        return jsonify(data)
