import os
import shutil
import click
import yaml

import pandas as pd

from omopetl.pipeline import run_etl


PROJECT_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates", "project")
DEMO_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates", "demo")


@click.group()
def cli():
    """OMOPETL: A package for managing OMOP ETL projects."""
    pass


@cli.command()
@click.argument("project_name")
def startproject(project_name):
    """Create a new ETL project folder."""
    project_path = os.path.abspath(project_name)
    if os.path.exists(project_path):
        print(f"Error: Directory '{project_name}' already exists.")
        return

    shutil.copytree(PROJECT_TEMPLATE_DIR, project_path)
    print(f"Project '{project_name}' created successfully at {project_path}")


@cli.command()
@click.argument("project_name")
def startdemo(project_name):
    """Create a demo ETL project for a MIMIC to OMOP transformation."""
    project_path = os.path.abspath(project_name)
    if os.path.exists(project_path):
        print(f"Error: Directory '{project_name}' already exists.")
        return

    shutil.copytree(DEMO_TEMPLATE_DIR, project_path)
    print(f"Demo project '{project_name}' created successfully at {project_path}")


@cli.command()
@click.argument("csv_directory", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(writable=True), default="source_schema.yaml")
def inferschema(csv_directory, output_file):
    """
    Infer a source_schema.yaml file from CSV files in a directory.

    Parameters:
    - csv_directory: Directory containing the CSV files.
    - output_file: Output YAML file to save the schema.
    """
    schema = {}

    for file_name in os.listdir(csv_directory):
        if file_name.endswith(".csv"):
            table_name = os.path.splitext(file_name)[0]
            file_path = os.path.join(csv_directory, file_name)
            df = pd.read_csv(file_path)

            # Infer schema
            columns = {}
            potential_primary_keys = []

            for col_name, dtype in df.dtypes.items():
                # Map Pandas dtypes to schema types
                if pd.api.types.is_integer_dtype(dtype):
                    col_type = "Integer"
                elif pd.api.types.is_float_dtype(dtype):
                    col_type = "Float"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    col_type = "DateTime"
                elif pd.api.types.is_bool_dtype(dtype):
                    col_type = "Boolean"
                else:
                    col_type = "String"

                # Add column to schema
                columns[col_name] = {"type": col_type}

                # Check uniqueness and non-null for potential primary key
                if df[col_name].is_unique and df[col_name].notnull().all():
                    potential_primary_keys.append(col_name)

            # Apply heuristics to select the primary key
            primary_key = None
            for key_candidate in potential_primary_keys:
                if key_candidate == "id" or key_candidate == f"{table_name}_id" or key_candidate.endswith("_id"):
                    primary_key = key_candidate
                    break
            if not primary_key and potential_primary_keys:
                # Default to the first unique column
                primary_key = potential_primary_keys[0]

            if primary_key:
                columns[primary_key]["primary_key"] = True

            schema[table_name] = {"table_name": table_name, "columns": columns}

    # Sort schema file alphabetically
    sorted_schema = dict(sorted(schema.items()))

    # Write schema to YAML with a header comment and empty lines between tables
    with open(output_file, "w") as yaml_file:
        # Add header
        yaml_file.write(
            "# This schema was automatically inferred by omopetl.\n"
            "# Please review and adjust it carefully before using.\n\n"
        )

        # Write schema with empty lines between tables
        yaml_file.write("\n".join(
            yaml.dump({table: sorted_schema[table]}, default_flow_style=False, sort_keys=False)
            for table in sorted_schema
        ))

    click.echo(f"Inferred schema saved to {output_file}. Please review it carefully.")


@cli.command()
@click.argument("project_path")
@click.option("--dry", is_flag=True, help="Run the ETL without saving the results (dry run).")
@click.option("--casual", is_flag=True, help="Relax validation rules and warnings.")
def run(project_path, dry, casual):
    """
    Run the ETL process for the specified project.

    Parameters:
    - project_path: Path to the project folder containing configurations and data.
    - --dry: If provided, runs the ETL without saving the output.
    - --casual: If provided, relaxes validation rules and prints warnings instead of raising errors.
    """
    project_path = os.path.abspath(project_path)
    if not os.path.exists(project_path):
        raise click.ClickException(f"Project path '{project_path}' does not exist.")

    # Execute the ETL pipeline
    run_etl(project_path, dry=dry, casual=casual)
    if dry:
        print("ETL executed in dry run mode. No data was saved.")
    else:
        print("ETL completed successfully.")


if __name__ == "__main__":
    cli()
