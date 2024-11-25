import os
import pandas as pd
from omopetl.transform import transform_data
from omopetl.utils import load_yaml


def extract_and_combine_data(config, source_tables, project_path):
    """
    Extract data from multiple source tables and combine them.

    Parameters:
    - config: The ETL configuration.
    - source_tables: List of source tables to extract data from.
    - project_path: The project folder path.

    Returns:
    - DataFrame: Combined data from all source tables.
    """
    combined_data = pd.DataFrame()
    source_dir = os.path.join(project_path, config['etl']['source']['directory'])

    for source_table in source_tables:
        file_path = os.path.join(source_dir, f"{source_table}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        data = pd.read_csv(file_path)
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    return combined_data


def load_data(config, data, target_table, project_path):
    """
    Load data into the target, resolving paths relative to the project path.

    Parameters:
    - config: The ETL configuration.
    - data: Transformed DataFrame.
    - target_table: Name of the target table.
    - project_path: The project folder path.
    """
    target_type = config['etl']['target']['type']
    target_dir = os.path.join(project_path, config['etl']['target']['directory'])

    if target_type == 'csv':
        os.makedirs(target_dir, exist_ok=True)
        output_path = os.path.join(target_dir, f"{target_table}.csv")
        data.to_csv(output_path, index=False)
    else:
        raise ValueError(f"Unsupported target type: {target_type}")


def run_etl(project_path, dry=False):
    """
    Main ETL pipeline.

    Parameters:
    - project_path: Path to the project folder.
    - dry: If True, run the ETL without saving the output (dry run).
    """
    config_path = os.path.join(project_path, "config", "etl_config.yaml")
    mappings_path = os.path.join(project_path, "config", "mappings.yaml")
    etl_config = load_yaml(config_path)
    mappings = load_yaml(mappings_path)

    for table_config in etl_config['etl']['mappings']:
        source_tables = table_config['source_tables']  # Support multiple source tables
        target_table = table_config['target_table']
        column_mappings = mappings[table_config['column_mappings']]

        print(f"Processing tables: {', '.join(source_tables)} -> {target_table}")
        data = extract_and_combine_data(etl_config, source_tables, project_path)
        transformed_data = transform_data(data, column_mappings)

        if not dry:
            load_data(etl_config, transformed_data, target_table, project_path)
            print(f"Saved transformed data for table: {target_table}")
        else:
            print(f"Dry run: Data for table '{target_table}' not saved.")
