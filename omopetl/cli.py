import click
import os
import shutil

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


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

    # Copy templates to the new project directory
    shutil.copytree(TEMPLATE_DIR, project_path)
    print(f"Project '{project_name}' created successfully at {project_path}")


if __name__ == "__main__":
    cli()
