"""Define methods for generating a PyArrow table from a project and/or repository."""

import copy
import itertools
from functools import singledispatch

import pyarrow as pa
import pyarrow.compute as pc
from lazyscribe import Project, Repository


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
    raw_ = pa.Table.from_pylist(list(obj))
    for name in ["created_at", "last_updated"]:
        col_index_ = raw_.column_names.index(name)
        new_ = pc.assume_timezone(
            raw_.column(name).cast(pa.timestamp("s")), timezone="UTC"
        )

        raw_ = raw_.set_column(
            col_index_, pa.field(name, pa.timestamp("s", tz="UTC")), new_
        )

    return raw_


@to_table.register(Repository)
def _(obj: Repository, /) -> pa.Table:
    """Convert a lazyscribe Repository to a PyArrow table.

    Parameters
    ----------
    obj : lazyscribe.Repository
        A lazyscribe Repository.

    Returns
    -------
    pyarrow.Table
        The PyArrow table.
    """
    # Need to create a unified schema -- get the total list of fields across handlers
    raw_data_ = list(obj)
    all_fields_ = set(itertools.chain.from_iterable([art.keys() for art in raw_data_]))
    parsed_data_: list[dict] = []
    for art in raw_data_:
        parsed_data_.append(copy.copy(art))
        for new_field_ in all_fields_.difference(set(art.keys())):
            parsed_data_[-1][new_field_] = None

    table_ = pa.Table.from_pylist(parsed_data_)
    # make ``created_at`` a timezone-aware timestamp column
    col_index_ = table_.column_names.index("created_at")
    new_ = pc.assume_timezone(
        table_.column("created_at").cast(pa.timestamp("s")), timezone="UTC"
    )

    return table_.set_column(
        col_index_, pa.field("created_at", pa.timestamp("s", tz="UTC")), new_
    )
