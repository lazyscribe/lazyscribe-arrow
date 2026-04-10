"""Custom serialization for the handlers themselves."""

import logging

import pyarrow as pa
from attrs import asdict
from lazyscribe.artifacts.base import Artifact
from pyarrow.interchange import from_dataframe

from lazyscribe_arrow.protocols import (
    ArrowArrayExportable,
    ArrowStreamExportable,
    SupportsInterchange,
)

LOG = logging.getLogger(__name__)


def _to_arrow(obj, /) -> pa.Table:
    """Convert the incoming object to a pyarrow Table.

    Light wrapper on existing pyarrow functionality. We're only abstracting it since
    we use the same block multiple times.

    Parameters
    ----------
    obj
        The object to convert.

    Returns
    -------
    pyarrow.Table
        The converted arrow object.

    Raises
    ------
    ValueError
        Raised if the supplied object does not have ``__arrow_c_array__``
        or ``__arrow_c_stream__`` attributes. These attributes allow us to
        perform a zero-copy transformation from the native obejct to a PyArrow
        Table.
    """
    match obj:
        case pa.Table():
            LOG.debug("Provided object is already a PyArrow table.")
            return obj
        case ArrowArrayExportable() | ArrowStreamExportable():
            return pa.table(obj)
        case SupportsInterchange():
            return from_dataframe(obj)
        case _:
            raise ValueError(
                f"Object of type `{type(obj)}` cannot be easily coerced into a PyArrow Table. "
                "Please provide an object that implements the Arrow PyCapsule Interface or the "
                "Dataframe Interchange Protocol."
            )


def _getstate(handler: Artifact, /) -> dict:
    """Serialize the artifact handler itself.

    Instead of using ``pickle``, we can use Arrow IPC.

    Parameters
    ----------
    handler : Artifact
        The ``csv`` or ``parquet`` file handler.

    Returns
    -------
    dict
        The state object, ready for serialization via ``pickle``.
    """
    state = asdict(handler)
    # Check for the existence of an artifact
    if (obj := state.get("value")) is not None:
        obj_ = _to_arrow(obj)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, obj_.schema) as writer:
            writer.write(obj_)
        state["value"] = sink.getvalue()

    return state


def _setstate(handler: Artifact, state: dict) -> None:
    """Deserialize the artifact handler using IPC.

    Parameters
    ----------
    handler : Artifact
        The Arrow-based artifact handler.
    state : dict
        The serialized state of the handler. We expect the output of
        :py:meth:`lazyscribe_arrow._utils._getstate`.
    """
    for key, value in state.items():
        match key:
            case "value":
                if value is not None:
                    with pa.ipc.open_file(value) as reader:
                        handler.value = reader.read_all()
                else:
                    handler.value = None
            case _:
                setattr(handler, key, value)
