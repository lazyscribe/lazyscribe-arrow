"""Test the custom CSV handler."""

import zoneinfo
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pytest
import time_machine
from pandas.testing import assert_frame_equal

from lazyscribe_arrow import CSVArtifact


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_csv_handler_pandas(tmp_path):
    """Test reading and writing CSV files with the handler and Pandas data."""
    location = tmp_path / "my-location"
    location.mkdir()

    data = pd.DataFrame({"key": ["value"]})
    handler = CSVArtifact.construct(name="My output file")

    assert handler.fname == "my-output-file-20250120132330.csv"

    with open(location / handler.fname, "wb") as buf:
        handler.write(data, buf)

    assert (location / handler.fname).is_file()

    with open(location / handler.fname, "rb") as buf:
        out = handler.read(buf)

    assert_frame_equal(data, out.to_pandas())


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_csv_handler_native(tmp_path):
    """Test reading and writing CSV files with the handler and native PyArrow objects."""
    location = tmp_path / "my-location"
    location.mkdir()

    data = pa.Table.from_pylist([{"key": "value"}])
    handler = CSVArtifact.construct(name="My output file")

    assert handler.fname == "my-output-file-20250120132330.csv"

    with open(location / handler.fname, "wb") as buf:
        handler.write(data, buf)

    assert (location / handler.fname).is_file()

    with open(location / handler.fname, "rb") as buf:
        out = handler.read(buf)

    assert_frame_equal(data.to_pandas(), out.to_pandas())


def test_csv_handler_raise_error(tmp_path):
    """Test raising an error based on a non-arrow object."""
    location = tmp_path / "my-empty-location"
    location.mkdir()

    data = [{"key": "value"}]
    handler = CSVArtifact.construct(name="My output file")

    with pytest.raises(ValueError), open(location / handler.fname, "wb") as buf:
        handler.write(data, buf)

    # we can't assert that the file does not exist since ``open`` creates the file,
    # even if there isn't any data
    assert (location / handler.fname).stat().st_size == 0


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_csv_dataframe_interchange(tmp_path):
    """Test using a data structure with dataframe interchange support."""

    class MyDataStructure:
        def __init__(self, data: pd.DataFrame):
            """Init method."""
            self.data = data

        def __dataframe__(self, nan_as_null: bool = False, allow_copy: bool = True):
            """The method for the dataframe interchange protocol."""
            return self.data.__dataframe__(
                nan_as_null=nan_as_null, allow_copy=allow_copy
            )

    location = tmp_path / "my-location"
    location.mkdir()

    data = pd.DataFrame({"key": ["value"]})
    handler = CSVArtifact.construct(name="My output file")

    assert handler.fname == "my-output-file-20250120132330.csv"

    with open(location / handler.fname, "wb") as buf:
        handler.write(MyDataStructure(data), buf)

    assert (location / handler.fname).is_file()

    with open(location / handler.fname, "rb") as buf:
        out = handler.read(buf)

    assert_frame_equal(data, out.to_pandas())
