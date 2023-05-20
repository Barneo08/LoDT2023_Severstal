import os
import sys

from src.web_service.web_app import app
import src.web_service.view as view


def start_server():
#    if os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), "ThisServerIsProductive"):
#        app.run(host="0.0.0.0")
#    else:
    app.run()
