Supported Engines
=================

SqlTask supports all databases that have a SqlAlchemy dialect and driver. The
following engines have dedicated support for the following insert modes:

+-------------+--------+----------+------+----------+
|             |        |  Insert mode    |          |
+             +--------+----------+------+----------+
| Database    | Single | Multirow | CSV  |  Parquet |
+=============+========+==========+======+==========+
| BigQuery    |   Yes  |    Yes   |  Yes |          |
+-------------+--------+----------+------+----------+
| Postgres    |   Yes  |    Yes   |  Yes |          |
+-------------+--------+----------+------+----------+
| Snowflake   |   Yes  |    Yes   |  Yes |          |
+-------------+--------+----------+------+----------+
| Sql Server  |   Yes  |    Yes   |      |          |
+-------------+--------+----------+------+----------+
| Sqlite      |   Yes  |    Yes   |      |          |
+-------------+--------+----------+------+----------+

Engines not listed above will default to using multirow inserts if supported,
falling back to single row inserts as a last resort.

Engine customization
####################

engine_params: Optional dict of parameter that get destructured as keyword arguments for create_engine call in the engine. Allows customization of engine connection pool and such.

For example, to set engine to use a connection pool size of one, pass engine_params = {"pool_size": 1}
