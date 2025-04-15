[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE) [![PyPI](https://img.shields.io/pypi/v/lazyscribe-arrow)](https://pypi.org/project/lazyscribe-arrow/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/lazyscribe-arrow)](https://pypi.org/project/lazyscrib-arrow/) [![codecov](https://codecov.io/gh/lazyscribe/lazyscribe-arrow/graph/badge.svg?token=W5TPK7GX7G)](https://codecov.io/gh/lazyscribe/lazyscribe-arrow)

# Arrow-based artifact handling for lazyscribe

`lazyscribe-arrow` is a lightweight package that adds the following artifact handlers for `lazyscribe`:

* `csv`

Any data structure that implements the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
will be compatible with the handlers in this library. Popular compatible open source data structures include

* `pandas.DataFrame`
* `polars.DataFrame`
* `polars.LazyFrame`

# Installation

Python 3.10 and above is required. use `pip` to install:

```console
$ python -m pip install lazyscribe-arrow
```

# Usage

To use this library, simply log an artifact to a `lazyscribe` experiment or repository with

* `handler="csv"` for a CSV output


```python
import pyarrow as pa
from lazyscribe import Project

project = Project("project.json", mode="w")
with project.log("My experiment") as exp:
    data = pa.Table.from_arrays([[0, 1, 2]], names=["a"])
    exp.log_artifact(name="data", value=data, handler="csv")

project.save()
```
