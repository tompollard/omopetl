import pandas as pd
import pytest
from omopetl.transform import Transformer


@pytest.fixture
def mock_transformer():
    """
    Fixture to create a Transformer instance with a mock target schema.
    """

    source_data = pd.DataFrame({
        "subject_id": [1, 2, 3],
        "race": ["White", "Black", "Asian"],
        "admittime": ["2020-01-01", "2020-02-01", "2020-03-01"]
    })

    target_schema = {
        "tmp_subject_race": {
            "columns": {
                "subject_id": {"type": "integer"},
                "admittime": {"type": "string"},
                "race": {"type": "string"}
            }
        }
    }

    return Transformer(source_data, "mock_project_path", {}, target_schema, "tmp_subject_race")


def test_transform_reorder_columns(mock_transformer):
    """Test if transformed data is reordered according to the target schema."""

    transformations = [
        {
            "add_column": "subject_id",
            "transformation": {"type": "copy", "source_column": "subject_id"}
        },
        {
            "add_column": "race",
            "transformation": {"type": "copy", "source_column": "race"}
        },
        {
            "add_column": "admittime",
            "transformation": {"type": "copy", "source_column": "admittime"}
        }
    ]

    transformed_data = mock_transformer.apply_transformations(transformations)
    expected_order = ["subject_id", "admittime", "race"]

    assert transformed_data.columns.to_list() == expected_order, \
        f"Expected column order {expected_order}, but got {list(transformed_data.columns)}"
