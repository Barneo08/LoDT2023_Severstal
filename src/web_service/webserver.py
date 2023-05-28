from src.web_service.web_app import app
import src.web_service.view as view
from src.config import *


def start_server():
    if CONFIG.WEB_SERVER_IS_PUBLIC:
        app.run(host="0.0.0.0")
    else:
        app.run()
