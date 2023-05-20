import src.constants as constants
import src.utils as utils


# запускается сервер обеспечивающий взаимодействие с пользователем:
utils.log_print("Starting server...")
if utils.if_all_modules_exist(constants.MODULES_PARAM_GROUPS["start"]):
    import src.web_service.webserver as webserver
    webserver.start_server()
else:
    utils.log_print("One of the modules is missing. The program is terminated.")
