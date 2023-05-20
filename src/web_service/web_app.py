from flask import Flask
from src.config import Configuraton

from src.web_service.predicts.blueprint import predicts

import src.db as db


app = Flask(__name__)
app.config.from_object(Configuraton)

if "db_connection" not in locals():
    db_connection = db.Data_handler()

app.register_blueprint(predicts, url_prefix="/predicts")
