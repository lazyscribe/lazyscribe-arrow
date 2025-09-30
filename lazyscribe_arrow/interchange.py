"""Define methods for generating a PyArrow table from a project and/or repository."""

from functools import singledispatch

import pyarrow as pa
from lazyscribe import Project
from slugify import slugify


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
    # Look for the superset of available parameters and metrics
    field_map_: dict[str, str] = {}
    for exp in obj.experiments:
        for param_, value_ in exp.parameters.items():
            if (param_slug_ := slugify(f"parameter-{param_}")) in fields:
                continue
            field_map_[param_slug_] = param_
            fields[param_slug_] = pa.scalar(value_).type
        for metric_, value_ in exp.metrics.items():
            if (metric_slug_ := slugify(f"metric-{metric_}")) in fields:
                continue
            field_map_[metric_slug_] = metric_
            fields[metric_slug_] = pa.scalar(value_).type

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
                data_[field.name] = [exp.parameters.get(field_map_[field.name], None)]
            elif field.name.startswith("metric-"):
                data_[field.name] = [exp.metrics.get(field_map_[field.name], pa.null())]
            else:
                continue

        batches.append(pa.record_batch(data_, schema=schema))

    return pa.Table.from_batches(batches, schema=schema)
