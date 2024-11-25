import os
import shutil
import click

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
@click.argument("project_path")
@click.option("--dry", is_flag=True, help="Run the ETL without saving the results (dry run).")
def run(project_path, dry):
    """
    Run the ETL process for the specified project.

    Parameters:
    - project_path: Path to the project folder containing configurations and data.
    - --dry: If provided, runs the ETL without saving the output.
    """
    project_path = os.path.abspath(project_path)
    if not os.path.exists(project_path):
        raise click.ClickException(f"Project path '{project_path}' does not exist.")

    # Execute the ETL pipeline
    run_etl(project_path, dry=dry)
    if dry:
        print("ETL executed in dry run mode. No data was saved.")
    else:
        print("ETL completed successfully.")


if __name__ == "__main__":
    cli()
