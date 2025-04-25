"""Import the custom artifact handlers."""

from lazyscribe_arrow.csv import CSVArtifact
from lazyscribe_arrow.parquet import ParquetArtifact

__all__: list[str] = ["CSVArtifact", "ParquetArtifact"]
