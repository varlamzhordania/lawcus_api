import logging
import os
from datetime import datetime

# Create a 'logs' folder if it doesn't exist
logs_folder = 'logs'
os.makedirs(logs_folder, exist_ok=True)

# Get the current date and time for the log file name
current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Configure the main log file with a handler that includes the log file location
app_log_file_path = os.path.join(logs_folder, f'app_{current_date}.log')
accounts_log_file_path = os.path.join(logs_folder, f'accounts_{current_date}.log')
activities_log_file_path = os.path.join(logs_folder, f'activities_{current_date}.log')
contacts_log_file_path = os.path.join(logs_folder, f'contacts_{current_date}.log')
interactions_log_file_path = os.path.join(logs_folder, f'interactions_{current_date}.log')
leads_log_file_path = os.path.join(logs_folder, f'leads_{current_date}.log')
matters_log_file_path = os.path.join(logs_folder, f'matters_{current_date}.log')
reports_log_file_path = os.path.join(logs_folder, f'reports_{current_date}.log')
tasks_log_file_path = os.path.join(logs_folder, f'tasks_{current_date}.log')
users_log_file_path = os.path.join(logs_folder, f'users_{current_date}.log')

# Configure the main log file
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# Create logger instances for each module
app_logger = logging.getLogger('app')
app_logger.setLevel(logging.INFO)
app_log_handler = logging.FileHandler(app_log_file_path)
app_log_handler.setFormatter(log_formatter)
app_logger.addHandler(app_log_handler)

accounts_logger = logging.getLogger('accounts')
accounts_logger.setLevel(logging.INFO)
accounts_log_handler = logging.FileHandler(accounts_log_file_path)
accounts_log_handler.setFormatter(log_formatter)
accounts_logger.addHandler(accounts_log_handler)

activities_logger = logging.getLogger('activities')
activities_logger.setLevel(logging.INFO)
activities_log_handler = logging.FileHandler(activities_log_file_path)
activities_log_handler.setFormatter(log_formatter)
activities_logger.addHandler(activities_log_handler)

contacts_logger = logging.getLogger('contacts')
contacts_logger.setLevel(logging.INFO)
contacts_log_handler = logging.FileHandler(contacts_log_file_path)
contacts_log_handler.setFormatter(log_formatter)
contacts_logger.addHandler(contacts_log_handler)

interactions_logger = logging.getLogger('interactions')
interactions_logger.setLevel(logging.INFO)
interactions_log_handler = logging.FileHandler(interactions_log_file_path)
interactions_log_handler.setFormatter(log_formatter)
interactions_logger.addHandler(interactions_log_handler)

leads_logger = logging.getLogger('leads')
leads_logger.setLevel(logging.INFO)
leads_log_handler = logging.FileHandler(leads_log_file_path)
leads_log_handler.setFormatter(log_formatter)
leads_logger.addHandler(leads_log_handler)

matters_logger = logging.getLogger('matters')
matters_logger.setLevel(logging.INFO)
matters_log_handler = logging.FileHandler(matters_log_file_path)
matters_log_handler.setFormatter(log_formatter)
matters_logger.addHandler(matters_log_handler)

reports_logger = logging.getLogger('reports')
reports_logger.setLevel(logging.INFO)
reports_log_handler = logging.FileHandler(reports_log_file_path)
reports_log_handler.setFormatter(log_formatter)
reports_logger.addHandler(reports_log_handler)

tasks_logger = logging.getLogger('tasks')
tasks_logger.setLevel(logging.INFO)
tasks_log_handler = logging.FileHandler(tasks_log_file_path)
tasks_log_handler.setFormatter(log_formatter)
tasks_logger.addHandler(tasks_log_handler)

users_logger = logging.getLogger('users')
users_logger.setLevel(logging.INFO)
users_log_handler = logging.FileHandler(users_log_file_path)
users_log_handler.setFormatter(log_formatter)
users_logger.addHandler(users_log_handler)
