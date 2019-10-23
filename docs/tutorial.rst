Tutorial - Creating a simple ETL task
=====================================

When creating a task, you should extend the SqlTask class, creating a
constructor that takes the parameters required for running a single idempotent
task. For a regular batch task this is usully the reporting date. It is also
perfectly fine to have no parameters; this is generally the case when populating
static dimension tables.

.. code-block:: python

   from datetime import date

   from sqltask import SqlTask


   class MyEtlTask(SqlTask):
       def __init__(report_date: date):
          super().__init__(report_date: report_date)

asdf
