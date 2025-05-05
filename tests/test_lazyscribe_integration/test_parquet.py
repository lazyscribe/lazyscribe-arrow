"""Test using the parquet handler with Lazyscribe."""

import zoneinfo
from datetime import datetime

import pandas as pd
import time_machine
from lazyscribe import Project
from pandas.testing import assert_frame_equal


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_parquet_project_write(tmp_path):
    """Test logging an artifact using the parquet handler."""
    location = tmp_path / "my-project-location"
    location.mkdir()

    project = Project(fpath=location / "project.json", mode="w")
    with project.log("My experiment") as exp:
        data = pd.DataFrame({"key": ["value"]})
        exp.log_artifact(name="My data", value=data, handler="parquet")

    project.save()

    assert (
        location / "my-experiment-20250120132330" / "my-data-20250120132330.parquet"
    ).is_file()

    project_r = Project(fpath=location / "project.json", mode="r")
    out = project_r["my-experiment"].load_artifact(name="My data")

    assert_frame_equal(data, out.to_pandas())
