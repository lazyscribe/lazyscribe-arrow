"""Test object interchange with Arrow."""

import sys
import zoneinfo
from datetime import datetime

import pyarrow as pa
import time_machine
from lazyscribe import Project, Repository

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
            "author": ["myself"],
            "last_updated_by": ["myself"],
            "metrics": [{"my-metric": 0.5}],
            "parameters": [{"my-param": 0, "my-list-param": [0, 1, 2]}],
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
            "dependencies": [[]],
            "short_slug": ["my-experiment"],
            "slug": ["my-experiment-20250120132330"],
            "tests": [[]],
            "artifacts": [[]],
            "tags": [[]],
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
            "author": ["myself", "myself"],
            "last_updated_by": ["myself", "myself"],
            "metrics": [{"my-metric": 0.5}, {"my-metric": 0.5}],
            "parameters": [
                {"my-param": 0, "my-list-param": [0, 1, 2]},
                {"my-features": ["a", "b", "c"]},
            ],
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
            "dependencies": [[], []],
            "short_slug": ["my-experiment", "second-experiment"],
            "slug": [
                "my-experiment-20250120132330",
                "second-experiment-20250120132330",
            ],
            "tests": [[], []],
            "artifacts": [[], []],
            "tags": [[], []],
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
            "author": ["myself"],
            "last_updated_by": ["myself"],
            "metrics": [{}],
            "parameters": [{}],
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
            "dependencies": [[]],
            "short_slug": ["my-experiment"],
            "slug": ["my-experiment-20250120132330"],
            "tests": [
                [
                    {
                        "name": "My test",
                        "description": "My population",
                        "metrics": {"my-metric": 0.5},
                        "parameters": {},
                    },
                    {
                        "name": "My second test",
                        "description": "My second population",
                        "metrics": {"my-metric": 0.25},
                        "parameters": {},
                    },
                ],
            ],
            "artifacts": [[]],
            "tags": [["has-tests"]],
        }
    )

    assert table == expected


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_project_to_table_art():
    """Test converting a project to a table with logged artifacts."""
    project = Project("project.json", author="myself", mode="w")
    with project.log("My experiment") as exp:
        exp.log_artifact(name="features", value=["a", "b", "c"], handler="json")

    table = to_table(project)

    expected = pa.table(
        {
            "name": ["My experiment"],
            "author": ["myself"],
            "last_updated_by": ["myself"],
            "metrics": [{}],
            "parameters": [{}],
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
            "dependencies": [[]],
            "short_slug": ["my-experiment"],
            "slug": ["my-experiment-20250120132330"],
            "tests": [[]],
            "artifacts": [
                [
                    {
                        "created_at": "2025-01-20T13:23:30",
                        "fname": "features-20250120132330.json",
                        "handler": "json",
                        "name": "features",
                        "python_version": ".".join(
                            str(i) for i in sys.version_info[:2]
                        ),
                        "version": 0,
                    }
                ]
            ],
            "tags": [[]],
        }
    )

    assert table == expected


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_repo_to_table_basic():
    """Test converting a basic repository to a table."""
    repo = Repository("repository.json", mode="w")
    repo.log_artifact(name="features", value=["a", "b", "c"], handler="json")

    table = to_table(repo)

    expected = pa.table(
        {
            "name": ["features"],
            "fname": ["features-20250120132330.json"],
            "created_at": [
                pa.scalar(
                    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")),
                    type=pa.timestamp("s", tz="UTC"),
                )
            ],
            "version": [0],
            "python_version": [".".join(str(i) for i in sys.version_info[:2])],
            "handler": ["json"],
        }
    )

    assert table == expected


@time_machine.travel(
    datetime(2025, 1, 20, 13, 23, 30, tzinfo=zoneinfo.ZoneInfo("UTC")), tick=False
)
def test_repo_to_table_mismatch():
    """Test converting a repository to a table with artifact handlers that have different fields."""
    repo = Repository("repository.json", mode="w")
    repo.log_artifact(name="features", value=["a", "b", "c"], handler="json")
    repo.log_artifact(
        name="data", value=pa.Table.from_pylist([{"key": "value"}]), handler="csv"
    )

    table = to_table(repo)

    expected = pa.table(
        {
            "name": ["features", "data"],
            "fname": ["features-20250120132330.json", "data-20250120132330.csv"],
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
            "version": [0, 0],
            "python_version": [".".join(str(i) for i in sys.version_info[:2]), None],
            "handler": ["json", "csv"],
        }
    )

    assert table == expected
