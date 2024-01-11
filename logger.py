import logging
import os
from datetime import datetime
from endpoint_run import endpoint_logger_config


def configure_logger(logger_name, log_file_path):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    log_handler = logging.FileHandler(log_file_path)
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_handler.setFormatter(log_formatter)

    logger.addHandler(log_handler)
    return logger


# Create a 'logs' folder if it doesn't exist
logs_folder = 'logs'
os.makedirs(logs_folder, exist_ok=True)

# Get the current date and time for the log file name
current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Configure loggers based on endpoint configuration
loggers = {}
for endpoint, run_flag in endpoint_logger_config.items():
    if run_flag:
        log_file_path = os.path.join(logs_folder, f'{endpoint}_{current_date}.log')
        loggers[endpoint] = configure_logger(endpoint, log_file_path)

# Export loggers for use in modules
app_logger = configure_logger("app", os.path.join(logs_folder, f'app_{current_date}.log'))
accounts_logger = loggers.get('accounts', None)
activities_logger = loggers.get('activities', None)
contacts_logger = loggers.get('contacts', None)
interactions_logger = loggers.get('interactions', None)
leads_logger = loggers.get('leadsources', None)
matters_logger = loggers.get('matters', None)
reports_logger = loggers.get('reports', None)
tasks_logger = loggers.get('tasks', None)
users_logger = loggers.get('users', None)
