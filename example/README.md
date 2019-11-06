# Examples

## customer.py - Business context

A data analyst/engineer has the task of populating a customer table.
The final customer table is expected to look like this:

| report_date|etl_timestamp|customer_id|birthdate|age|blood_group |
| ---- | ---| --- | --- | --- | --- |
||

The task will consist of joining two source queries:
* customers
* customer_blood_groups

Blood groups are to be validated via a lookup table of valid blood groups:
* valid_blood_groups.csv

The sink table is
* fact_customer 

As part of the ETL process a data quality table is created:
* customer_dq

## Setting up sqlalchemy engines

In the example database urls are retrieved from environment variables,
defaulting to `sqlite`. This can be replaced by any SqlAlchemy connection 
that supports ANSI SQL syntax:

### bash (Mac, Linux)

```bash
export SQLTASK_SOURCE="sqlite:///source.db" 
export SQLTASK_TARGET="sqlite:///target.db"
```

### powershell (Windows)

```powershell
$env:SQLTASK_TARGET = "sqlite:///target.db"
$env:SQLTASK_SOURCE = "sqlite:///source.db"
```

##  Running ETL

```bash
python run_example.py
```

## Viewing results

### Table fact_customer

The sink table can be viewed with the following script:

```bash
echo "select * from fact_customer;"  | sqlite3 --column --header target.db
```


|report_date|etl_timestamp|customer_id|birthdate|age|blood_group|
| --- | --- | --- | --- | --- | --- |
2019-06-30|2019-11-06 05:59:52.380735|Terminator|||
2019-06-30|2019-11-06 05:59:52.380654|Sarah Connor|1956-09-26|62|A+
2019-06-30|2019-11-06 05:59:52.380425|Peter Impossible|||
2019-06-30|2019-11-06 05:59:52.380202|Mary Null|||
2019-06-30|2019-11-06 05:59:52.378324|John Connor|||A-

### Table fact_customer_dq

As part of the task data quality issues are logged in a separate table 
`fact_customer_dq`. The results can be viewed with the following script:

```bash
echo "select * from fact_customer_dq;"  | sqlite3 --column --header target.db
```

|report_date|customer_id|rowid|source|priority|category|column_name|message|
| --- | --- | --- | --- | --- | --- | --- | --- |
2019-06-30|Terminator|2019-11-06 05:59:52.380884|source|high|incorrect|blood_group|Invalid blood group: Liquid Metal
2019-06-30|Terminator|2019-11-06 05:59:52.380825|transform|medium|missing|age|Age is undefined due to undefined birthdate
2019-06-30|Terminator|2019-11-06 05:59:52.380767|source|high|incorrect|birthdate|Birthdate in future: 2095-01-01
2019-06-30|Peter Impossible|2019-11-06 05:59:52.380575|source|high|incorrect|blood_group|Invalid blood group: X+
2019-06-30|Peter Impossible|2019-11-06 05:59:52.380516|transform|medium|missing|age|Age is undefined due to undefined birthdate
2019-06-30|Peter Impossible|2019-11-06 05:59:52.380459|source|high|incorrect|birthdate|Cannot parse birthdate: 1980-13-01
2019-06-30|Mary Null|2019-11-06 05:59:52.380341|source|medium|missing|blood_group|Blood group undefined in customer blood group table
2019-06-30|Mary Null|2019-11-06 05:59:52.380280|transform|medium|missing|age|Age is undefined due to undefined birthdate
2019-06-30|Mary Null|2019-11-06 05:59:52.380219|source|medium|missing|birthdate|Missing birthdate
2019-06-30|John Connor|2019-11-06 05:59:52.378454|transform|medium|missing|age|Age is undefined due to undefined birthdate
2019-06-30|John Connor|2019-11-06 05:59:52.378385|source|high|incorrect|birthdate|Birthdate in future: 2080-01-01
