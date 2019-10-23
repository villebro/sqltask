.. sqltask documentation master file, created by
   sphinx-quickstart on Mon Oct 14 20:52:17 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to sqltask's documentation!
'''''''''''''''''''''''''''''''''''

Sqltask is an extensible ETL library based on [SqlAlchemy](https://www.sqlalchemy.org/)
with the intent of enabling building robust ETL pipelines with high emphasis on 
data quality.

Main features of Sqltask:
=========================
- Create well documented data models that support iterative
  development of both schema and data transformation logic.
- Combine data quality checking with transformation logic with automated 
  creation of visualization-friendly data quality tables.
- Make use of SQL where practical, especially expensive data filtering 
  and aggregation during data extraction.
- Row-by-row data transformation using Python where SQL falls short,
  e.g. calling third party libraries or storing state from previous rows.
- Encourage use of modern version control tools and processes, especially GIT.
- Performant data uploading/insertion where supported.
- Easy integration with modern ETL orchestration tools, especially
  [Apache Airflow](https://airflow.apache.org/).


.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
