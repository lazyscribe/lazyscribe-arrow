"""Test using the CSV handler with Lazyscribe."""

import zoneinfo
from datetime import datetime
from importlib.metadata import version

import pandas as pd
import pyarrow.dataset as ds
import pytest
import time_machine
from lazyscribe import Project
from packaging.version import Version
from pandas.testing import assert_frame_equal

LAZY_VERSION = Version(version("lazyscribe"))


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_csv_project_write(tmp_path):
    """Test logging an artifact using the CSV handler."""
    location = tmp_path / "my-project-location"
    location.mkdir()

    project = Project(fpath=location / "project.json", mode="w")
    with project.log("My experiment") as exp:
        data = pd.DataFrame({"key": ["value"]})
        exp.log_artifact(name="My data", value=data, handler="csv")

    project.save()

    assert (
        location / "my-experiment-20250120132330" / "my-data-20250120132330.csv"
    ).is_file()

    project_r = Project(fpath=location / "project.json", mode="r")
    out = project_r["my-experiment"].load_artifact(name="My data")

    assert_frame_equal(data, out.to_pandas())


@pytest.mark.skipif(
    Version("2.4.0") > LAZY_VERSION,
    reason="Artifact scanning requires lazyscribe 2.4.0",
)
@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_csv_project_scan(tmp_path):
    """Test scanning an artifact using the CSV handler."""
    location = tmp_path / "my-project-location"
    location.mkdir()

    project = Project(fpath=location / "project.json", mode="w")
    with project.log("My experiment") as exp:
        data = pd.DataFrame({"key": ["value"]})
        exp.log_artifact(name="My data", value=data, handler="csv")

    project.save()

    project_r = Project(fpath=location / "project.json", mode="r")
    out = project_r["my-experiment"].scan_artifact(name="My data")

    assert isinstance(out, ds.Dataset)
    assert_frame_equal(data, out.to_table().to_pandas())
