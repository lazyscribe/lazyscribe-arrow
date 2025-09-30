"""Test object interchange with Arrow."""

import zoneinfo
from datetime import datetime

import pyarrow as pa
import time_machine
from lazyscribe import Project

from lazyscribe_arrow.interchange import to_table


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_project_to_table_basic():
    """Test converting a project to a table."""
    project = Project("project.json", author="myself", mode="w")
    with project.log("My experiment") as exp:
        exp.log_parameter("my-param", 0)
        exp.log_parameter("my-list-param", [0, 1, 2])
        exp.log_metric("my-metric", 0.5)

    table = to_table(project)

    expected = pa.table(
        {
            "name": ["My experiment"],
            "slug": ["my-experiment-20250120132330"],
            "short_slug": ["my-experiment"],
            "author": ["myself"],
            "created_at": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                )
            ],
            "last_updated": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                )
            ],
            "last_updated_by": ["myself"],
            "tags": [pa.scalar([], type=pa.list_view(pa.string()))],
            "parameter-my-param": [0],
            "parameter-my-list-param": [[0, 1, 2]],
            "metric-my-metric": [0.5],
            "tests": [
                pa.scalar(
                    [],
                    type=pa.list_view(
                        pa.struct({"name": pa.string(), "description": pa.string()})
                    ),
                )
            ],
        }
    )

    assert table == expected


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_project_to_table_mismatch_params():
    """Test converting a project to a table with different parameters in each experiment."""
    project = Project("project.json", author="myself", mode="w")
    with project.log("My experiment") as exp:
        exp.log_parameter("my-param", 0)
        exp.log_parameter("my-list-param", [0, 1, 2])
        exp.log_metric("my-metric", 0.5)
    with project.log("Second experiment") as exp:
        exp.log_parameter("my-features", ["a", "b", "c"])
        exp.log_metric("my-metric", 0.5)

    table = to_table(project)

    expected = pa.table(
        {
            "name": ["My experiment", "Second experiment"],
            "slug": [
                "my-experiment-20250120132330",
                "second-experiment-20250120132330",
            ],
            "short_slug": ["my-experiment", "second-experiment"],
            "author": ["myself", "myself"],
            "created_at": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                ),
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                ),
            ],
            "last_updated": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                ),
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                ),
            ],
            "last_updated_by": ["myself", "myself"],
            "tags": [
                pa.scalar([], type=pa.list_view(pa.string())),
                pa.scalar([], type=pa.list_view(pa.string())),
            ],
            "parameter-my-param": [0, None],
            "parameter-my-list-param": [[0, 1, 2], None],
            "metric-my-metric": [0.5, 0.5],
            "parameter-my-features": [None, ["a", "b", "c"]],
            "tests": [
                pa.scalar(
                    [],
                    type=pa.list_view(
                        pa.struct({"name": pa.string(), "description": pa.string()})
                    ),
                ),
                pa.scalar(
                    [],
                    type=pa.list_view(
                        pa.struct({"name": pa.string(), "description": pa.string()})
                    ),
                ),
            ],
        }
    )

    assert table == expected


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_project_to_table_no_parameters():
    """Test converting a project to a table with no parameters and/or metrics."""
    project = Project("project.json", author="myself", mode="w")
    with project.log("My experiment") as exp:
        pass

    table = to_table(project)

    expected = pa.table(
        {
            "name": ["My experiment"],
            "slug": ["my-experiment-20250120132330"],
            "short_slug": ["my-experiment"],
            "author": ["myself"],
            "created_at": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                )
            ],
            "last_updated": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                )
            ],
            "last_updated_by": ["myself"],
            "tags": [pa.scalar([], type=pa.list_view(pa.string()))],
            "tests": [
                pa.scalar(
                    [],
                    type=pa.list_view(
                        pa.struct({"name": pa.string(), "description": pa.string()})
                    ),
                )
            ],
        }
    )

    assert table == expected


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_project_to_table_tests():
    """Test converting a project to a table with logged tests."""
    project = Project("project.json", author="myself", mode="w")
    with project.log("My experiment") as exp:
        exp.tag("has-tests")
        with exp.log_test(name="My test", description="My population") as test_:
            test_.log_metric("my-metric", 0.5)
        with exp.log_test(
            name="My second test", description="My second population"
        ) as test_:
            test_.log_metric("my-metric", 0.25)

    table = to_table(project)

    expected = pa.table(
        {
            "name": ["My experiment"],
            "slug": ["my-experiment-20250120132330"],
            "short_slug": ["my-experiment"],
            "author": ["myself"],
            "created_at": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                )
            ],
            "last_updated": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                )
            ],
            "last_updated_by": ["myself"],
            "tags": [pa.scalar(["has-tests"], type=pa.list_view(pa.string()))],
            "tests": [
                pa.scalar(
                    [
                        {
                            "name": "My test",
                            "description": "My population",
                            "metric-my-metric": 0.5,
                        },
                        {
                            "name": "My second test",
                            "description": "My second population",
                            "metric-my-metric": 0.25,
                        },
                    ],
                    type=pa.list_view(
                        pa.struct(
                            {
                                "name": pa.string(),
                                "description": pa.string(),
                                "metric-my-metric": pa.float64(),
                            }
                        )
                    ),
                )
            ],
        }
    )

    assert table == expected
