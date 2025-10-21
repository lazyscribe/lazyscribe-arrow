"""Check that basic features work.

Used in our publishing pipeline.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
from lazyscribe import Project

with tempfile.TemporaryDirectory() as tmpdir:
    # Create a project
    project = Project(Path(tmpdir) / "project.json", mode="w")
    with project.log(name="Arrow experiment") as exp:
        # Create a fake object and log it
        data = pa.Table.from_pylist([{"key": "value"}])
        exp.log_artifact(name="data-csv", value=data, handler="csv")
        exp.log_artifact(name="data-parquet", value=data, handler="parquet")

    project.save()

    exp = project["arrow-experiment"]
    data_csv_ = exp.load_artifact(name="data-csv")
    data_parquet_ = exp.load_artifact(name="data-parquet")

    assert data_csv_ == data
    assert data_parquet_ == data
