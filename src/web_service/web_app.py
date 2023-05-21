from flask import Flask
from src.config import FlaskConfiguration

from src.web_service.predicts.blueprint import predicts

import src.db as db


app = Flask(__name__)
app.config.from_object(FlaskConfiguration)

if "db_connection" not in locals():
    db_connection = db.DataHandler()

app.register_blueprint(predicts, url_prefix="/predicts")