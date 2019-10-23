# Examples

## customer.py - Business context

A data analyst/engineer has the task of populating a customer table.
The final customer table is expected to look like this:

| report_date|etl_timestamp|customer_id|birthdate|age|sector_code |
| ---- | ---| --- | --- | --- | --- |
||


The task will consist of joining two source queries:
* customers
* sector_code

sink table is
* fact_customer 

As part of the ETL process a data quality table is created:
* customer_dq


## Setting up sqlalchemy engines

In the example database urls are retrieved from environment variables,
defaulting to `sqlite` (can be replaced by any SqlAlchemy connection 
string):

### bash

```bash
EXPORT SQLTASK_SOURCE="sqlite:///source.db" 
EXPORT SQLTASK_TARGET="sqlite:///target.db"
```

### powershell

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


|report_date|etl_timestamp|customer_id|birthdate|age|sector_code|
| --- | --- | --- | --- | --- | --- |
|2019-06-30|2019-10-09 13:28:06.081927|1234567|1980-01-01|39.4934976043806|111211|
|2019-06-30|2019-10-09 13:28:06.090929|2345678|||143|
|2019-06-30|2019-10-09 13:28:06.090929|2245678||||
|2019-06-30|2019-10-09 13:28:06.090929|3456789||||

### Table fact_customer_dq

As part of the task data quality issues are logged in a separate table 
`fact_customer_dq`. The results can be viewed with the following script:

```bash
echo "select * from fact_customer_dq;"  | sqlite3 --column --header target.db
```

|report_date|customer_id|rowid|source|priority|category|column_name|message|
| --- | --- | --- | --- | --- | --- | --- | --- |
2019-06-30|2245678|4a167eb2-34d5-4473-af18-e4238dedf2e3|source|high|incorrect|birthdate|Cannot parse birthdate: 1980-13-01
2019-06-30|2245678|654326b4-3442-431e-85c7-525d1ffea97a|transform|medium|missing|age|Age is undefined due to undefined birthdate
2019-06-30|2245678|6ed6d51c-2d70-4f12-91d8-6d86fd6d14b3|source|low|missing|sector_code|Sector code undefined
2019-06-30|2345678|e307dfd0-7b01-4fcc-b09e-7f52aaf4820a|source|high|incorrect|birthdate|Birthdate in future: 2080-01-01
2019-06-30|2345678|b16c9337-abdd-4dce-b03b-bbddc6dca875|transform|medium|missing|age|Age is undefined due to undefined
2019-06-30|3456789|ff04ebef-76f1-48e9-a314-2eb05e9f9c41|source|medium|missing|birthdate|Missing birthdate
2019-06-30|3456789|ab197fa2-9c69-4bca-acd4-ebafc5eacbda|transform|medium|missing|age|Age is undefined due to undefined
