# Examples

## customer.py - Business context

Dataengineer has a task of populating table customer.

customer table looks like this

| report_date|etl_timestamp|customer_id|birthdate|age|sector_code |
| ---- | ---| --- | --- | --- | --- |
||


In his sqltask he will be joining two source  queries
* main
* sector_code

sink table is
* customer 

as part of process dq-table is created
* customer_dq


## Setting up environment variables

Example gets database url:s from environment variables

### bash

```bash
SQLTASK_SOURCE="sqlite:///source.db" 
EXPORT SQLTASK_SOURCE
SQLTASK_TARGET="sqlite:///target.db"  
EXPORT SQLTASK_TARGET
```

### powershell

```powershell
$env:SQLTASK_TARGET = "sqlite:///target.db"
$env:SQLTASK_SOURCE = "sqlite:///source.db"
```


##  Running ETL


```bash
python customer.py
```

## Viewing results

### Table customer

Sink table can be viewed with following script:

```bash
echo "select * from customer;"  | sqlite3 --column --header target.db
```


|report_date|etl_timestamp|customer_id|birthdate|age|sector_code|
| --- | --- | --- | --- | --- | --- |
|2019-06-30|2019-10-09 13:28:06.081927|1234567|1980-01-01|39.4934976043806|111211|
|2019-06-30|2019-10-09 13:28:06.090929|2345678|||143|
|2019-06-30|2019-10-09 13:28:06.090929|2245678||||
|2019-06-30|2019-10-09 13:28:06.090929|3456789||||


### Table customer_dq

As part of sql-task generated data quality tests are run.  Result of these can be found from table customer_dq

```bash
echo "select * from customer_dq;"  | sqlite3 --column --header target.db
```

|report_date|customer_id|dq_rowid|source|priority|dq_type|column_name|
| --- | --- | --- | --- | --- | --- | --- |
|2019-06-30||046f3866-2164-4d04-b67c-e22f3b92465c|lookup|medium|duplicate|
|2019-06-30|2345678|c2d9f19e-d40f-4085-86ea-0867687d4705|source|high|incorrect|birthdate|
|2019-06-30|2345678|24b042ae-9f5b-4c23-af6a-aff1d7c62330|transform|medium|missing|age|
|2019-06-30|2245678|1f67a9bd-254a-4e84-9d72-924ba4562189|source|high|incorrect|birthdate|
|2019-06-30|2245678|d63a1444-3944-4630-ba29-e6e063362ad7|transform|medium|missing|age|
|2019-06-30|2245678|52968437-7922-418c-997c-09d2bfc69e7c|source|medium|missing|sector_code|
|2019-06-30|3456789|571031b0-fbc3-4cd6-9841-3eff91e7b238|source|high|missing|birthdate|
|2019-06-30|3456789|2d8c8bd5-0166-4487-b4e4-02554cb8440b|transform|medium|missing|age|
|2019-06-30|3456789|402fc8db-2eff-48da-9755-2aa23780affb|source|medium|missing|sector_code|

