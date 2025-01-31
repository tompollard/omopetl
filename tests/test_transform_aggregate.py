import pytest
import pandas as pd
from omopetl.transform import Transformer


@pytest.fixture
def sample_admissions():
    """
    Fixture to provide sample admissions data.
    """
    return pd.DataFrame({
        "subject_id": [1, 1, 2, 2, 3, 3],
        "admittime": [
            "2023-01-01", "2023-01-02",
            "2023-01-01", "2023-01-03",
            "2023-01-04", "2023-01-02"
        ],
        "race": ["White", "Black", "Asian", "Hispanic", "Other", "Unknown"]
    })


@pytest.fixture
def mock_transformer_race(sample_admissions):
    """
    Fixture to initialize the Transformer with sample data.
    """
    project_path = "/mock_project"
    source_schema = {"admissions": {"columns": {"subject_id": {"type": "integer"}, "admittime": {"type": "date"}, "race": {"type": "string"}}}}
    target_schema = {"tmp_subject_race": {"columns": {"subject_id": {"type": "integer"}, "race": {"type": "string"}}}}

    return Transformer(sample_admissions, project_path, source_schema, target_schema, "tmp_subject_race")


def test_transform_aggregate(mock_transformer_race, sample_admissions):
    """
    Test aggregation transformation that selects the first race
    based on admittime per subject_id.
    """
    transformation = {
        "target_column": "race",
        "source_columns": ["subject_id", "admittime", "race"],
        "group_by": ["subject_id"],
        "order_by": "admittime",
        "aggregation": "first"
    }

    expected_output = pd.Series(["White", "Asian", "Unknown"], name="race", index=[0, 2, 4])

    transformed_data = mock_transformer_race.transform_aggregate(sample_admissions, "race", transformation)

    pd.testing.assert_series_equal(
        transformed_data.iloc[[0, 2, 4]].reset_index(drop=True),
        expected_output.reset_index(drop=True),
        check_index=False
    )
