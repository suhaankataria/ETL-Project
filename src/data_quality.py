import pandas as pd
import re
from src.utils import setup_logging

class DataQualityChecker:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logging(self.config['log_file'])
        self.gibberish_regex = re.compile(self.config['data_quality_checks']['gibberish_product_name_regex'])

    def check_product_name_quality(self, df, product_col='product'):
        if product_col in df.columns:
            issue_mask = df[product_col].astype(str).apply(lambda x: bool(self.gibberish_regex.search(x)))
            df['product_name_issue'] = False
            df.loc[issue_mask, 'product_name_issue'] = True
            issue_count = df['product_name_issue'].sum()
            total_count = len(df)
            issue_percentage = (issue_count / total_count) * 100 if total_count > 0 else 0
            self.logger.warning(f"Found {issue_count} ({issue_percentage:.2f}%) product names with potential issues.")
        else:
            self.logger.warning(f"Product column '{product_col}' not found for quality check.")
            return df, 0
        return df, issue_percentage

    def check_acid_properties(self, df, customer_id_col, order_id_col, date_col, product_col):
        # Basic check for potential inconsistencies (can be expanded)
        duplicate_orders = df.duplicated(subset=[customer_id_col, order_id_col, product_col, date_col], keep=False).sum()
        total_rows = len(df)
        duplicate_percentage = (duplicate_orders / total_rows) * 100 if total_rows > 0 else 0
        self.logger.info(f"Found {duplicate_orders} ({duplicate_percentage:.2f}%) potential duplicate order records.")
        return duplicate_percentage

    def identify_columns(self, df):
        mapping = self.config['column_mapping']
        checks = self.config['data_quality_checks']

        id_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in checks['id_columns_keywords'])]
        order_id_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in checks['order_id_columns_keywords'])]
        product_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in checks['product_columns_keywords'])]
        date_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in checks['date_columns_keywords'])]

        return id_cols[0] if id_cols else None, \
               order_id_cols[0] if order_id_cols else None, \
               product_cols[0] if product_cols else None, \
               date_cols[0] if date_cols else None