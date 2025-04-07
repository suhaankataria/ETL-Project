import pandas as pd
import matplotlib.pyplot as plt
import os
from src.utils import setup_logging

class DataAnalyzer:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logging(self.config['log_file'])
        self.report_dir = self.config['report_directory']
        os.makedirs(self.report_dir, exist_ok=True)

    def generate_issue_report(self, issue_percentage):
        report_path = os.path.join(self.report_dir, 'data_quality_report.txt')
        with open(report_path, 'w') as f:
            f.write("Data Quality Report\n")
            f.write("----------------------\n")
            f.write(f"Percentage of records with potential product name issues: {issue_percentage:.2f}%\n")
            # Add more details if needed

        self.logger.info(f"Data quality report saved to: {report_path}")

    def generate_issue_pie_chart(self, issue_percentage):
        labels = 'Issues', 'No Issues'
        sizes = [issue_percentage, 100 - issue_percentage]
        colors = ['#ff9999', '#66b3ff']
        explode = (0.1, 0)  # explode 1st slice

        plt.figure(figsize=(8, 8))
        plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow=True, startangle=140)
        plt.title('Percentage of Records with Product Name Issues')
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

        chart_path = os.path.join(self.report_dir, 'product_issue_pie_chart.png')
        plt.savefig(chart_path)
        plt.close()
        self.logger.info(f"Product issue pie chart saved to: {chart_path}")