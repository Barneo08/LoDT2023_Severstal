from src.web_service.web_app import app
from flask import render_template


@app.route("/")
def index():
    title = "Стартовая страница"
    content_title = "content_title"
    content = "Здесь должен быть контент. Вот список:"

    stocks_list = [["First", "1111", "Иванов"], ["Second", "2222", "Петров"], ["Third", "3333", "Сидоров"]]
    my_list = []
    for one_stock in stocks_list:
        my_list = my_list + [["predicts/frame_" + one_stock[0], one_stock[2]]]

    return render_template("index.html", title=title, content_title=content_title, content=content, html_list=my_list)
