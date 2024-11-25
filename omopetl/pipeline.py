import os
import pandas as pd
from omopetl.transform import transform_data
from omopetl.utils import load_yaml


def extract_data(config, source_table, project_path):
    """Extract data from the source, resolving paths relative to the project path."""
    source_type = config['etl']['source']['type']
    source_dir = os.path.join(project_path, config['etl']['source']['directory'])
    if source_type == 'csv':
        file_path = os.path.join(source_dir, f"{source_table}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return pd.read_csv(file_path)
    raise ValueError(f"Unsupported source type: {source_type}")


def load_data(config, data, target_table, project_path):
    """Load data into the target, resolving paths relative to the project path."""
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
        source_table = table_config['source_table']
        target_table = table_config['target_table']
        column_mappings = mappings[table_config['column_mappings']]

        print(f"Processing table: {source_table} -> {target_table}")
        data = extract_data(etl_config, source_table, project_path)
        transformed_data = transform_data(data, column_mappings)

        if not dry:
            load_data(etl_config, transformed_data, target_table, project_path)
            print(f"Saved transformed data for table: {target_table}")
        else:
            print(f"Dry run: Data for table '{target_table}' not saved.")
