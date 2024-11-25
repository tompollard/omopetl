import os
import shutil
import click


# Paths to templates
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


if __name__ == "__main__":
    cli()
