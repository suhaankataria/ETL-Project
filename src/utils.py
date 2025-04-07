import logging
import yaml
import os

def setup_logging(log_file):
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger()

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)