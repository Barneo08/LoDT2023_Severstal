from flask import Flask
from src.config import FlaskConfiguration

from src.web_service.predicts.blueprint import predicts

app = Flask(__name__)
app.config.from_object(FlaskConfiguration)

app.register_blueprint(predicts, url_prefix="/predicts")
