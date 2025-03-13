import pandas as pd
import pytest
from omopetl.transform import Transformer


@pytest.fixture
def sample_data():
    """Creates a sample DataFrame with an admittime column."""
    return pd.DataFrame({
        "subject_id": [1, 2, 3, 4],
        "number": [1, 2, 3, 4]
    })


def test_multistep_transformations_same_column(sample_data):
    """Test applying multiple transformations sequentially to a single column without removing rows."""

    # Mock schemas
    mock_source_schema = {
        "test_table": {
            "columns": {
                "subject_id": {"type": "integer"},
                "number": {"type": "integer"}
            }
        }
    }

    mock_target_schema = {
        "test_table": {
            "columns": {
                "number_new": {"type": "integer"}
            }
        }
    }

    transformer = Transformer(
        data=sample_data,
        project_path="test_project",
        source_schema=mock_source_schema,
        target_schema=mock_target_schema,
        table_name="test_table"
    )

    # Define multi-step transformation on the same column
    transformations = [
        {
            "add_column": "number_new",
            "transformation": [
                {
                    "type": "derive",
                    "source_column": "number",
                    "formula": "number + 1"
                },
                {
                    "type": "derive",
                    "source_column": "number",
                    "formula": "number + 1"
                }
            ]
        }
    ]

    # Apply transformations
    transformed_data = transformer.apply_transformations(transformations)

    # Expected output: subject_id + 2
    expected_output = pd.DataFrame({
        "number_new": [3, 4, 5, 6]
    })

    expected_output["number_new"] = expected_output["number_new"].astype("Int64")

    # Validate the output
    pd.testing.assert_frame_equal(transformed_data, expected_output)
