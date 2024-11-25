import os


def list_files_in_directory(directory, ignore_dirs=None):
    """
    Recursively list all files in a directory, with relative paths.

    Parameters:
        directory (str): The directory to list files from.
        ignore_dirs (list): List of directories to ignore (relative to the root).

    Returns:
        list: A sorted list of file paths relative to the input directory.
    """
    if ignore_dirs is None:
        ignore_dirs = []

    files = []
    for root, dirnames, filenames in os.walk(directory):
        # Filter out ignored directories
        dirnames[:] = [d for d in dirnames if os.path.relpath(os.path.join(root, d), directory) not in ignore_dirs]

        for filename in filenames:
            relative_path = os.path.relpath(os.path.join(root, filename), directory)
            files.append(relative_path)
    return sorted(files)


def test_project_and_demo_match():
    """Ensure all files in the project template match those in the demo template, ignoring certain folders."""
    base_dir = os.path.dirname(__file__)  # Adjust based on test location
    project_template_dir = os.path.join(base_dir, "..", "templates", "project")
    demo_template_dir = os.path.join(base_dir, "..", "templates", "demo")

    # List files, ignoring the "data" directory
    ignore_dirs = ["data"]
    project_files = list_files_in_directory(project_template_dir, ignore_dirs)
    demo_files = list_files_in_directory(demo_template_dir, ignore_dirs)

    # Assert that the file lists are identical
    assert project_files == demo_files, "File lists in 'project' and 'demo' templates do not match."
