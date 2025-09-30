"""Define methods for generating a PyArrow table from a project and/or repository."""

from functools import singledispatch
from typing import Any, Protocol

import pyarrow as pa
from lazyscribe import Project
from slugify import slugify


class HasMetricsParameters(Protocol):
    """Class that has a parameters iterable."""

    metrics: dict[str, int | float]
    parameters: dict[str, Any]


def _gen_metric_parameters_fields(
    obj: HasMetricsParameters, fields: dict, field_map: dict[str, str]
) -> tuple[dict, dict]:
    """Generate the schema for parameters and/or metrics.

    Parameters
    ----------
    obj : list[HasMetricsParameters]
        A list of objects that has metrics and parameters.
    fields : dict
        The existing schema object.
    field_map : dict
        A dictionary mapping the field slug name to the attribute name in the
        source object.

    Returns
    -------
    dict
        The updated ``fields`` dictionary.
    dict
        The field map.
    """
    for param_, value_ in obj.parameters.items():
        if (param_slug_ := slugify(f"parameter-{param_}")) in fields:
            continue
        field_map[param_slug_] = param_
        fields[param_slug_] = pa.scalar(value_).type
    for metric_, value_ in obj.metrics.items():
        if (metric_slug_ := slugify(f"metric-{metric_}")) in fields:
            continue
        field_map[metric_slug_] = metric_
        fields[metric_slug_] = pa.scalar(value_).type

    return fields, field_map


@singledispatch
def to_table(obj, /) -> pa.Table:
    """Convert a lazyscribe Project or Repository to a PyArrow table.

    Parameters
    ----------
    obj : lazyscribe.Project | lazyscribe.Repository
        The object to convert.

    Returns
    -------
    pyarrow.Table
        The PyArrow table.
    """


@to_table.register(Project)
def _(obj: Project, /) -> pa.Table:
    """Convert a lazyscribe Project to a PyArrow table.

    Parameters
    ----------
    obj : lazyscribe.Project
        A lazyscribe project.

    Returns
    -------
    pyarrow.Table
        The PyArrow table.
    """
    # Create the schema
    fields = {
        "name": pa.string(),
        "slug": pa.string(),
        "short_slug": pa.string(),
        "author": pa.string(),
        "created_at": pa.timestamp("s", tz="UTC"),
        "last_updated": pa.timestamp("s", tz="UTC"),
        "last_updated_by": pa.string(),
        "tags": pa.list_view(pa.string()),
    }
    test_fields = {"name": pa.string(), "description": pa.string()}

    # Look for the superset of available parameters and metrics
    field_map_: dict[str, str] = {}
    test_field_map_: dict[str, str] = {}
    for exp in obj.experiments:
        fields, field_map_ = _gen_metric_parameters_fields(exp, fields, field_map_)
        # Look at the tests
        for test_ in exp.tests:
            test_fields, test_field_map_ = _gen_metric_parameters_fields(
                test_, test_fields, test_field_map_
            )
    fields["tests"] = pa.list_view(pa.struct(test_fields))

    schema = pa.schema(fields)
    batches: list[pa.RecordBatch] = []
    for exp in obj.experiments:
        data_ = {
            "name": [exp.name],
            "slug": [exp.slug],
            "short_slug": [exp.short_slug],
            "author": [exp.author],
            "created_at": [exp.created_at],
            "last_updated": [exp.last_updated],
            "last_updated_by": [exp.last_updated_by],
            "tags": [exp.tags],
        }

        for field in schema:
            if field.name.startswith("parameter-"):
                data_[field.name] = [exp.parameters.get(field_map_[field.name])]
            elif field.name.startswith("metric-"):
                data_[field.name] = [exp.metrics.get(field_map_[field.name])]
            else:
                continue

        exp_test_: list[dict] = []
        for test_ in exp.tests:
            test_data_ = {"name": test_.name, "description": test_.description}
            for field_name_ in test_fields:
                if field_name_.startswith("parameter-"):
                    test_data_[field_name_] = test_.parameters.get(
                        test_field_map_[field_name_]
                    )
                elif field_name_.startswith("metric-"):
                    test_data_[field_name_] = test_.metrics.get(
                        test_field_map_[field_name_]
                    )
                else:
                    continue

            exp_test_.append(test_data_)

        data_["tests"] = [exp_test_]

        batches.append(pa.record_batch(data_, schema=schema))

    return pa.Table.from_batches(batches, schema=schema)
