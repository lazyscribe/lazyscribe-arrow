"""Arrow exportable protocols."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ArrowArrayExportable(Protocol):
    """Type protocol for Arrow C Data Interface via Arrow PyCapsule Interface."""

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]:
        """Export the object as a pair of ArrowSchema and ArrowArray structures."""
        ...


@runtime_checkable
class ArrowStreamExportable(Protocol):
    """Type protocol for Arrow C Stream Interface via Arrow PyCapsule Interface."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """Export the object as an ArrowArrayStream."""
        ...


@runtime_checkable
class SupportsInterchange(Protocol):
    """Dataframe that supports conversion into an interchange dataframe object."""

    def __dataframe__(
        self,
        nan_as_null: bool = False,
        allow_copy: bool = True,
    ) -> SupportsInterchange:
        """Convert to a dataframe object implementing the dataframe interchange protocol."""
        ...
