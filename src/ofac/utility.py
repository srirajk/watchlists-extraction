import json
import os

def load_config():
    config_file = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

from prettytable import PrettyTable

def pretty_print_spark_df(df, limit=10):
    # Convert to Pandas for better control
    pandas_df = df.limit(limit).toPandas()
    table = PrettyTable()
    table.field_names = pandas_df.columns
    for row in pandas_df.itertuples(index=False):
        table.add_row(row)
    print(table)
