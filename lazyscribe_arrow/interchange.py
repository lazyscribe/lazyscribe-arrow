"""Define methods for generating a PyArrow table from a project and/or repository."""

from functools import singledispatch

import pyarrow as pa
import pyarrow.compute as pc
from lazyscribe import Project


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
