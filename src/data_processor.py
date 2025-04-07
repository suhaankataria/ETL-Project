import pandas as pd
from src.utils import setup_logging

class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logging(self.config['log_file'])

    def map_columns(self, df):
        mapping = self.config['column_mapping']
        column_map = {}
        for internal_name, external_hint in mapping.items():
            found_cols = [col for col in df.columns if external_hint.lower() in col.lower()]
            if found_cols:
                column_map[found_cols[0]] = internal_name
            else:
                self.logger.warning(f"Could not find column matching hint: '{external_hint}'.")
        return df.rename(columns=column_map, errors='ignore')

    def consolidate_orders(self, df, customer_id_col='username', order_id_col='order_id', product_col='product_name', date_col='order_date'):
        if not all([customer_id_col in df.columns, product_col in df.columns, date_col in df.columns]):
            self.logger.error("Required columns for consolidation not found.")
            return pd.DataFrame()

        df['product_with_date'] = df.apply(lambda row: (row[product_col], pd.to_datetime(row[date_col]).strftime('%Y-%m-%d') if pd.notna(row[date_col]) else None), axis=1)

        if order_id_col in df.columns:
            grouped = df.groupby(customer_id_col).agg(
                all_products=('product_with_date', 'unique')
            ).reset_index()
        else:
            grouped = df.groupby(customer_id_col).agg(
                all_products=('product_with_date', 'unique')
            ).reset_index()

        grouped.rename(columns={'all_products': 'products_ordered'}, inplace=True)
        return grouped