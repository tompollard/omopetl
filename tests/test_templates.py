import os


def list_files_in_directory(directory):
    """Recursively list all files in a directory, with relative paths."""
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            relative_path = os.path.relpath(os.path.join(root, filename), directory)
            files.append(relative_path)
    return sorted(files)


def test_project_and_demo_match():
    """Ensure all files in the project template match those in the demo template."""
    base_dir = os.path.dirname(__file__)
    project_template_dir = os.path.join(base_dir, "..", "templates", "project")
    demo_template_dir = os.path.join(base_dir, "..", "templates", "demo")

    # Get lists of files
    project_files = list_files_in_directory(project_template_dir)
    demo_files = list_files_in_directory(demo_template_dir)

    # Assert that the file lists are identical
    assert project_files == demo_files, "File lists in 'project' and 'demo' templates do not match."

