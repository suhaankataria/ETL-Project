from src.data_loader import DataLoader
from src.data_processor import DataProcessor
from src.data_quality import DataQualityChecker
from src.data_analyzer import DataAnalyzer
from src.utils import load_config, create_directory_if_not_exists, setup_logging
import os
'''
if __name__ == "__main__":
    config = load_config('D:\etl_project\config\config,yaml')
    logger = config['logging'] = load_config('D:\etl_project\config\config,yaml')['log_file']

    # Setup directories
    create_directory_if_not_exists(config['output_directory'])
    create_directory_if_not_exists(config['log_file'].rsplit('/', 1)[0])
    create_directory_if_not_exists(config['report_directory'])

    data_loader = DataLoader(config)
    data_processor = DataProcessor(config)
    data_quality_checker = DataQualityChecker(config)
    data_analyzer = DataAnalyzer(config)

    # Load Data
    raw_data = data_loader.load_data()
    if raw_data.empty:
        logger.warning("No data loaded. Exiting.")
        exit()

    # Identify Columns
    customer_id_col, order_id_col, product_col, date_col = data_quality_checker.identify_columns(raw_data.copy())
    if not all([customer_id_col, product_col, date_col]):
        logger.error("Could not identify essential columns. Check column name hints in config.")
        exit()

    # Data Quality Checks
    processed_data, issue_percentage = data_quality_checker.check_product_name_quality(raw_data.copy(), product_col)
    acid_issue_percentage = data_quality_checker.check_acid_properties(processed_data.copy(), customer_id_col, order_id_col, date_col, product_col) # More comprehensive ACID checks can be added

    # Map Columns
    mapped_data = data_processor.map_columns(processed_data.copy())

    # Consolidate Orders
    consolidated_data = data_processor.consolidate_orders(mapped_data.copy(), customer_id_col, order_id_col, 'product_name', 'order_date')

    # Save Processed Data (Optional)
    output_file = os.path.join(config['output_directory'], 'consolidated_orders.csv')
    consolidated_data.to_csv(output_file, index=False)
    logger.info(f"Consolidated data saved to: {output_file}")

    # Generate Reports
    data_analyzer.generate_issue_report(issue_percentage)
    data_analyzer.generate_issue_pie_chart(issue_percentage)

    logger.info("ETL process completed.")
    '''
if __name__ == "__main__":
    config = load_config('config,yaml')

    # Setup logging correctly
    log_file_path = config['log_file']
    logger = setup_logging(log_file_path)
    config['logging'] = logger # Optionally store the logger in the config

    # Setup directories
    create_directory_if_not_exists(config['output_directory'])
    create_directory_if_not_exists(log_file_path.rsplit('/', 1)[0])
    create_directory_if_not_exists(config['report_directory'])

    data_loader = DataLoader(config)
    data_processor = DataProcessor(config)
    data_quality_checker = DataQualityChecker(config)
    data_analyzer = DataAnalyzer(config)

    # Load Data
    raw_data = data_loader.load_data()
    if raw_data.empty:
        logger.warning("No data loaded. Exiting.")
        exit()

    # Identify Columns
    customer_id_col, order_id_col, product_col, date_col = data_quality_checker.identify_columns(raw_data.copy())
    if not all([customer_id_col, product_col, date_col]):
        logger.error("Could not identify essential columns. Check column name hints in config.")
        exit()

    # Data Quality Checks
    processed_data, issue_percentage = data_quality_checker.check_product_name_quality(raw_data.copy(), product_col)
    acid_issue_percentage = data_quality_checker.check_acid_properties(processed_data.copy(), customer_id_col, order_id_col, date_col, product_col) # More comprehensive ACID checks can be added

    # Map Columns
    mapped_data = data_processor.map_columns(processed_data.copy())

    # Consolidate Orders
    consolidated_data = data_processor.consolidate_orders(mapped_data.copy(), customer_id_col, order_id_col, 'product_name', 'order_date')

    # Save Processed Data (Optional)
    output_file = os.path.join(config['output_directory'], 'consolidated_orders.csv')
    consolidated_data.to_csv(output_file, index=False)
    logger.info(f"Consolidated data saved to: {output_file}")

    # Generate Reports
    data_analyzer.generate_issue_report(issue_percentage)
    data_analyzer.generate_issue_pie_chart(issue_percentage)

    logger.info("ETL process completed.")