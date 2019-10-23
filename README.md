[![PyPI version](https://img.shields.io/pypi/v/sqltask.svg)](https://badge.fury.io/py/sqltask)
[![PyPI](https://img.shields.io/pypi/pyversions/sqltask.svg)](https://www.python.org/downloads/)
[![Build Status](https://travis-ci.com/villebro/sqltask.svg?branch=master)](https://travis-ci.com/villebro/sqltask)
[![codecov](https://codecov.io/gh/villebro/sqltask/branch/master/graph/badge.svg)](https://codecov.io/gh/villebro/sqltask)
[![Requirements Status](https://requires.io/github/villebro/sqltask/requirements.svg?branch=master)](https://requires.io/github/villebro/sqltask/requirements/?branch=master)
[![Documentation](https://readthedocs.org/projects/sqltask/badge/?version=latest)](https://sqltask.readthedocs.io/en/latest/)
[![PyPI license](https://img.shields.io/pypi/l/sqltask.svg)](https://opensource.org/licenses/MIT)
# Sqltask
Sqltask is an extensible ETL library based on [SqlAlchemy](https://www.sqlalchemy.org/)
to help build robust ETL pipelines with high emphasis on data quality.

Main features of Sqltask:
- Create well documented data models that support iterative
development of both schema and data transformation logic.
- Tightly coupled data quality checking with transformation logic with automatic
creation of visualization-friendly and actionable data quality tables.
- Make use of SQL where practical, especially expensive data filtering 
and aggregation during data extraction.
- Row-by-row data transformation using Python where SQL falls short,
e.g. calling third party libraries or storing state from previous rows.
- Encourage use of modern version control tools and processes, especially GIT.
- Performant data uploading/insertion where supported.
- Easy integration with modern ETL orchestration tools, especially
[Apache Airflow](https://airflow.apache.org/).

# Supported databases

Sqltask supports all databases with a SqlAlchemy
[dialect](https://docs.sqlalchemy.org/en/13/dialects/), with
dedicated support for the following engines:
- Google BigQuery (experimental)
- MS SQL Server (experimental)
- Postgres
- Sqlite
- Snowflake

Engines not listed above will fall back to using regular inserts.

## Installation instructions

To install Sqltask without any dependencies, simply run

```bash
pip install sqltask
```

To automatically pull in dependencies needed by Snowflake, type
```bash
pip install sqltask[snowflake]
```

Please refer to the [example case](https://github.com/villebro/sqltask/tree/master/example)
to see how Sqltask can be used in practice.
