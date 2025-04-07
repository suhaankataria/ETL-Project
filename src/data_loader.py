import pandas as pd
import glob
import os
from src.utils import setup_logging

class DataLoader:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logging(self.config['log_file'])

    def load_data(self):
        all_data = []
        input_dir = self.config['input_directory']
        file_patterns = ['*.csv', '*.xlsx', '*.xls']

        for pattern in file_patterns:
            for file_path in glob.glob(os.path.join(input_dir, pattern)):
                self.logger.info(f"Loading data from: {file_path}")
                try:
                    if file_path.endswith('.csv'):
                        df = pd.read_csv(file_path)
                    else:
                        df = pd.read_excel(file_path)
                    all_data.append(df)
                except Exception as e:
                    self.logger.error(f"Error loading {file_path}: {e}")
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()