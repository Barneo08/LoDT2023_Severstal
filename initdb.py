import src.constants as constants
import src.utils as utils


# запускается блок по созданию и инициализации базы данных:
module_name = "Init DB"
utils.log_print("Starting initialisation of DBase...", module_name=module_name)
if utils.if_all_modules_exist(constants.MODULES_PARAM_GROUPS["initdb"]):
    import src.db as db
    db.DataHandler(keep_silence=False)
else:
    utils.log_print("One of the modules is missing. The program is terminated.", module_name=module_name)
