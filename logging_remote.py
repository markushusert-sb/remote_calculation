import logging
import os
import datetime

class CustomLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # Create a file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def log_event(self, message):
        if "Substituting" in message:
            raise Exception("caught the perpetrator")
        self.logger.info(message)

    def show_logs(self):
        with open(self.log_file, 'r') as file:
            logs = file.read()
            print(logs)

    def delete_old_entries(self, days_to_keep=7):
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_to_keep)
        with open(self.log_file, 'r+') as file:
            lines = file.readlines()
            file.seek(0)
            for line in lines:
                log_date_str = line.split(' - ')[0]
                log_date = datetime.datetime.strptime(log_date_str, '%Y-%m-%d %H:%M:%S')
                if log_date >= cutoff_date:
                    file.write(line)
            file.truncate()

# Example usage
logger=CustomLogger(os.path.join(os.path.dirname(os.path.realpath(__file__)),"remote_calc_logs.log"))
if False:
    logger = CustomLogger('event_log.txt')
    logger.log_event('Event 1')
    logger.log_event('Event 2')
    logger.show_logs()
    logger.delete_old_entries(7)  # Delete entries older than 7 days

