[![PyPI version](https://img.shields.io/pypi/v/sqltask.svg)](https://badge.fury.io/py/sqltask)
[![PyPI](https://img.shields.io/pypi/pyversions/sqltask.svg)](https://www.python.org/downloads/)
[![PyPI license](https://img.shields.io/pypi/l/sqltask.svg)](https://opensource.org/licenses/MIT)
# Sqltask
Sqltask is an ETL library based on [SqlAlchemy](https://www.sqlalchemy.org/)
to produce batch ETL processing with emphasis on the following:
- Ability to create well documented data models that support iterative
development of both schema and data transformation logic.
- Possibility to combine data quality checking with transformation logic,
with automatic creation of visualization-friendly data quality tables.
- Promote use of SQL where practical, especially expensive and complex data
filtering and aggregation tasks during extraction.
- Row-by-row data transformation using Python where SQL isn't practical.
- Encourage use of modern version control tools and processed, especially GIT.
- Performant data loading using bulk-loading where supported.
- Easy integration with modern ETL orchestration tools, especially
[Apache Airflow](https://airflow.apache.org/).

# Supported databases

Sqltask supports all databases with a
[Sqlalchemy dialect](https://docs.sqlalchemy.org/en/13/dialects/), with
performant bulk-loading for the following engines:
- Google BigQuery (experimental)
- MS SQL Server (experimental)
- Postgres
- Sqlite
- Snowflake

Engines not listed above will fall back to using regular inserts.

# Installation instructions

To install Sqltask without any dependencies, simply run

```bash
pip install sqltask
```

To automatically install all supported third party modules type
```bash
pip install sqltask[bigquery,mssql,snowflake,postgres]
```
