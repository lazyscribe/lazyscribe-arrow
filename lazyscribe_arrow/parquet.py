"""Custom artifact handlers for parquets."""

import logging
from datetime import datetime
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
from attrs import define
from lazyscribe._utils import utcnow
from lazyscribe.artifacts.base import Artifact
from pyarrow.interchange import from_dataframe
from slugify import slugify

LOG = logging.getLogger(__name__)


@define(auto_attribs=True)
class ParquetArtifact(Artifact):
    """Arrow-powered Parquet handler."""

    alias: ClassVar[str] = "parquet"
    suffix: ClassVar[str] = "parquet"
    binary: ClassVar[bool] = True
    output_only: ClassVar[bool] = False

    @classmethod
    def construct(
        cls,
        name: str,
        value: Any | None = None,
        fname: str | None = None,
        created_at: datetime | None = None,
        writer_kwargs: dict | None = None,
        version: int = 0,
        dirty: bool = True,
        **kwargs,
    ):
        """Construct the handler class."""
        created_at = created_at or utcnow()

        return cls(  # type: ignore[call-arg]
            name=name,
            value=value,
            fname=fname
            or f"{slugify(name)}-{slugify(created_at.strftime('%Y%m%d%H%M%S'))}.{cls.suffix}",
            writer_kwargs=writer_kwargs or {},
            version=version,
            created_at=created_at,
            dirty=dirty,
        )

    @classmethod
    def read(cls, buf, **kwargs) -> pa.Table:
        """Read in the parquet file.

        Parameters
        ----------
        buf : file-like object
            The buffer from a ``fsspec`` filesystem.
        **kwargs
            Keyword arguments for the read method.

        Returns
        -------
        pyarrow.lib.Table
            A ``pyarrow`` table with the data.
        """
        return pq.read_table(buf, **kwargs)

    @classmethod
    def write(cls, obj, buf, **kwargs):
        """Write the parquet file using pyarrow.

        Parameters
        ----------
        obj : object
            The object to write.
        buf : file-like object
            The buffer from a ``fsspec`` filesystem.
        **kwargs
            Keyword arguments for :py:meth:`pyarrow.parquet.write_table`.

        Raises
        ------
        ValueError
            Raised if the supplied object does not have ``__arrow_c_array__``
            or ``__arrow_c_stream__`` attribute
            or if the object does not
            implement the dataframe interchange protocol. These attributes allow us to
            perform a zero-copy transformation from the native obejct to a PyArrow
            Table.
        """
        if isinstance(obj, pa.Table):
            LOG.debug("Provided object is already a PyArrow table.")
        elif hasattr(obj, "__arrow_c_array__") or hasattr(obj, "__arrow_c_stream__"):
            obj = pa.table(obj)
        elif hasattr(obj, "__dataframe__"):
            obj = from_dataframe(obj)
        else:
            raise ValueError(
                f"Object of type `{type(obj)}` cannot be easily coerced into a PyArrow Table. "
                "Please provide an object that implements the Arrow PyCapsule Interface or the "
                "Dataframe Interchange Protocol."
            )

        pq.write_table(obj, buf, **kwargs)
