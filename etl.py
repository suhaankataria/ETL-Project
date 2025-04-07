import pandas as pd
import os
import glob

def process_excel_files_in_directory(directory_path):
    """
    Looks for CSV and Excel files in the specified directory, processes them,
    and consolidates customer order data.

    Args:
        directory_path (str): The path to the directory containing the files.

    Returns:
        pandas.DataFrame or None: A consolidated DataFrame of customer orders
                                  if files are found and processed successfully,
                                  otherwise None.
    """
    all_data = []
    file_patterns = ['*.csv', '*.xlsx', '*.xls']  # Add more patterns if needed

    found_files = False
    for pattern in file_patterns:
        for file_path in glob.glob(os.path.join(directory_path, pattern)):
            found_files = True
            print(f"Processing file: {file_path}")
            try:
                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:  # .xlsx or .xls
                    df = pd.read_excel(file_path)

                # Determine the columns dynamically (same logic as before)
                id_columns = [col for col in df.columns if 'customer' in col.lower() or 'user' in col.lower()]
                order_id_columns = [col for col in df.columns if 'order' in col.lower() and 'id' in col.lower()]
                product_columns = [col for col in df.columns if 'product' in col.lower() or 'item' in col.lower() or 'description' in col.lower()]
                date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]

                if not id_columns:
                    print(f"Warning: Could not find a customer/user identification column in {file_path}. Skipping.")
                    continue
                customer_id_col = id_columns[0]

                if not date_columns:
                    df['products_ordered'] = df.apply(
                        lambda row: [(row[prod], None) for prod in product_columns if prod in row and pd.notna(row[prod])],
                        axis=1
                    )
                    grouped = df.groupby(customer_id_col)['products_ordered'].sum().reset_index()
                    grouped.rename(columns={'products_ordered': 'products_ordered'}, inplace=True)
                elif not product_columns:
                    print(f"Warning: Could not find any product related columns in {file_path}. Skipping.")
                    continue
                else:
                    df['products_ordered_with_date'] = df.apply(
                        lambda row: [(row[prod], row[date])
                                      for prod in product_columns
                                      if prod in row and pd.notna(row[prod])
                                      for date in date_columns
                                      if date in row and pd.notna(row[date])],
                        axis=1
                    )
                    if order_id_columns:
                        order_id_col = order_id_columns[0]
                        grouped = df.groupby(customer_id_col).agg(
                            all_products=('products_ordered_with_date', 'sum')
                        ).reset_index()
                        grouped['unique_products_ordered'] = grouped['all_products'].apply(lambda x: list(set(x)))
                        temp_df = grouped[[customer_id_col, 'unique_products_ordered']]
                        temp_df.rename(columns={'unique_products_ordered': 'products_ordered'}, inplace=True)
                        all_data.append(temp_df)
                    else:
                        temp_df = df.groupby(customer_id_col)['products_ordered_with_date'].sum().reset_index()
                        temp_df.rename(columns={'products_ordered_with_date': 'products_ordered'}, inplace=True)
                        all_data.append(temp_df)

            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    if not found_files:
        print(f"No CSV or Excel files found in the directory: {directory_path}")
        return None
    elif not all_data:
        print("No data could be processed from the files.")
        return None
    else:
        # Concatenate all processed DataFrames
        consolidated_df = pd.concat(all_data, ignore_index=True)

        # Further aggregation to combine results from different files for the same customer
        final_consolidated_df = consolidated_df.groupby(customer_id_col)['products_ordered'].sum().reset_index()
        return final_consolidated_df

# Example usage:
directory_path = 'data_files'  # Replace with the path to your directory

# Create a dummy directory and files for testing
os.makedirs(directory_path, exist_ok=True)
data1 = {'CustomerID': [1, 1, 2], 'OrderDate': ['2025-04-01', '2025-04-01', '2025-04-02'], 'Product': ['Laptop', 'Keyboard', 'Mouse']}
df1 = pd.DataFrame(data1)
df1.to_csv(os.path.join(directory_path, 'orders1.csv'), index=False)

data2 = {'User': [1, 3], 'PurchaseDate': ['2025-04-03', '2025-04-04'], 'Item': ['Monitor', 'Webcam']}
df2 = pd.DataFrame(data2)
df2.to_excel(os.path.join(directory_path, 'orders2.xlsx'), index=False)

consolidated_orders_df = process_excel_files_in_directory(directory_path)

if consolidated_orders_df is not None:
    print("\nConsolidated Customer Orders from all files:")
    print(consolidated_orders_df)

    # Clean up the dummy directory and files (optional)
    import shutil
    shutil.rmtree(directory_path)