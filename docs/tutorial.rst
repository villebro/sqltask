Tutorial - Creating a simple ETL task
=====================================

This tutorial shows how to construct a typical ETL task. A fully functional
example can be found in the main repo of SqlTask:
`<https://github.com/villebro/sqltask/tree/master/example>`_

Introduction
------------

When creating a task, you will start by extending the `sqltask.SqlTask` class.
A task constitutes the the sequential execution of the following stages defined
as methods:

1. `__init__(**kwargs)`: define the target table(s), row source(s) and lookup
   source(s). `kwargs` denote the batch parameters based on which a single snapshot
   is run, e.g. `report_date=date(2019, 12, 31)`.
2. `transform()`: All transformation operations, i.e. reading inputs row-by-row and
   mapping values (transformed or not) to the output columns. During transformation
   data can be read from multiple sources, and can be mapped to multiple output tables,
   depending on what the transformation logic is. During transformation row-by-row
   data quality issues can be logged to the output table if using the `DqTableContext`
   target table class.
3. `validate()`: Post transformation data validation step, where the final output rows
   can be validated prior to insertion. In contrast to the data quality logging in
   the transform phase, validation should be done on an aggregate level, i.e. checking
   that row counts are in line with what is acceptable, null counts are acceptable
   etc.
4. `delete_rows()`: If an exception hasn't been raised before this step, the rows
   corresponding to the batch parameters will be deleted from the target table. If
   the task is defined to have one batch parameter `report_date`, this step in
   practice issues a `DELETE FROM tbl WHERE report_date = 'yyyy-mm-dd'` statement.
5. `insert_rows()`: This step inserts any rows that have been appended to the
   output tables using whichever insertion mode has been specified. Generic
   SqlAlchemy drivers will fall back to single or multirow inserts if supported,
   but engines with dedicated upload support will perform file-based uploading.

Base task
---------

For DAGs consisting of multiple tasks, it is commonly a good idea to create a base
task on which all tasks in the DAG are based, fixing the batch parameters in the
constructor as follows:

.. code-block:: python

   from datetime import date

   from sqltask import SqlTask


   class MyBaseTask(SqlTask):
       def __init__(report_date: date):
          super().__init__(report_date: report_date)

This way developers will have less ambiguity on which parameters the DAG tasks are
based on. For a regular batch task this is usully the date of the snapshot in question.
It is also perfectly fine to have no parameters or multiple parameters. Typical
scenarios:

- No parameters: Initialization of static dimension tables
- Single parameter: Calculation of a single snapshot, typically the snapshot date
- Multiple parameters: If data is further partitioned, it might be feasible to
  split up the calculation into further batches, e.g. per region, per hour.

In this example, the the unit of work for the task constitutes creating a single
snapshot for a certain `report_date`.

Creating an actual task
-----------------------

In the following example, we will construct a task that outputs data into a single
target table, reads data from a SQL query and uses a CSV table as a lookup table.
The class is based on `MyBaseTask` defined above. We will do the following

- Define a target table `my_table` based on `DqTableContext` into which data is
  inserted.
- Define a `SqlRowSource` instance that reads data from a SQL query.
- Define a `CsvLookupSource` instance that is used as a lookup table.

We have chosed `DqTableContext` as our target table class, as it can be used for
logging data quality issues. If we have our primary row data in CSV format, we
could also have used a `CsvRowSource` instance as the primary data source. Similarly
we could also use `SqlLookupSource` to construct our lookup table from a SQL query.

.. code-block:: python

    class MyTask(MyBaseTask):
        def __init__(self, report_date: date):
            super().__init__(report_date)

            # Define the metadata for the main fact table
            self.add_table(DqTableContext(
                name="my_table",
                engine_context=self.ENGINE_TARGET,
                columns=[
                    Column("report_date", Date, comment="Date of snapshot", primary_key=True),
                    Column("etl_timestamp", DateTime, comment="Timestamp when the row was created", nullable=False),
                    Column("customer_name", String(10), comment="Unique customer identifier (name)", primary_key=True),
                    Column("birthdate", Date, comment="Birthdate of customer if defined and in the past", nullable=True),
                    Column("age", Integer, comment="Age of customer in years if birthdate defined", nullable=True),
                    Column("blood_group", String(3), comment="Blood group of the customer", nullable=True),
                ],
                comment="The customer table",
                timestamp_column_name="etl_timestamp",
                batch_params={"report_date": report_date},
                dq_info_column_names=["etl_timestamp"],
            ))

TBC
