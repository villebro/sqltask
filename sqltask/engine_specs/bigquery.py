import logging
import os
from typing import Optional

from sqltask.classes.table import TableContext
from sqltask.engine_specs.base import BaseEngineSpec, UploadType
from sqltask.utils.engine_specs import create_tmp_csv


class BigQueryEngineSpec(BaseEngineSpec):
    engine = 'bigquery'
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = False

    @classmethod
    def _insert_rows_csv(cls, table_context: "TableContext") -> None:
        cls._insert_rows(table_context, UploadType.CSV)

    @classmethod
    def _insert_rows(cls, table_context: TableContext,
                     upload_type: Optional[UploadType] = None) -> None:
        """
        BigQuery bulk loading is done by exporting the data to CSV and using the
        `google-cloud-bigquery` library to upload data.

        :param table_context: the target table to upload into
        :param upload_type: If undefined, defaults to whichever ´UploadType` is defined
        in `default_upload_type`.
        """
        from google.cloud import bigquery

        file_path = create_tmp_csv(table_context)
        client = bigquery.Client()
        database = table_context.engine_context.engine.url.database
        schema = table_context.schema
        table_id = table_context.table.name

        # dataset_ref = client.dataset(file_path)
        # table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 0
        job_config.autodetect = True

        table_id = f"{database}.{schema}.{table_id}"

        try:
            with open(file_path, 'r', encoding="utf-8", newline='') as csv_file:
                job = client.load_table_from_file(
                    csv_file, table_id, job_config=job_config)
        finally:
            job.result()
            logging.info(f"Loaded {job.output_rows} rows into {table_id}")
            os.remove(f"{file_path}")
