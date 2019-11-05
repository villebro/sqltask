import logging
import os
from typing import Optional, Set

from sqltask.base.table import BaseTableContext
from sqltask.engine_specs.base import BaseEngineSpec, UploadType
from sqltask.utils.engine_specs import create_tmp_csv


class BigQueryEngineSpec(BaseEngineSpec):
    engine = 'bigquery'
    default_upload_type = UploadType.CSV
    supported_uploads: Set[UploadType] = {
        UploadType.SQL_INSERT,
        UploadType.CSV,
    }
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = False
    empty_where_clause = " WHERE 1 = 1"

    @classmethod
    def _insert_rows_csv(cls, table_context: "BaseTableContext") -> None:
        cls._insert_rows(table_context, UploadType.CSV)

    @classmethod
    def _insert_rows(cls, table_context: BaseTableContext,
                     upload_type: Optional[UploadType] = None) -> None:
        """
        BigQuery bulk loading is done by exporting the data to CSV and using the
        `google-cloud-bigquery` library to upload data.

        :param table_context: the target table to upload into
        :param upload_type: If undefined, defaults to whichever ´UploadType` is defined
        in `default_upload_type`.
        """
        from google.cloud import bigquery

        upload_type = upload_type or cls.default_upload_type
        if upload_type == UploadType.CSV:
            file_path = create_tmp_csv(table_context, delimiter=",")
        else:
            raise Exception(f"Unsupported upload type: {upload_type.name}")

        client = bigquery.Client()
        database = table_context.engine_context.engine.url.database
        table_id = table_context.table.name

        dataset_ref = client.dataset(database)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.skip_leading_rows = 0

        try:
            with open(file_path, 'rb') as csv_file:
                job = client.load_table_from_file(
                    csv_file, table_ref, job_config=job_config)
                job.result()
                logging.info(f"Loaded {job.output_rows} rows into {database}:{table_id}")
        finally:
            os.remove(f"{file_path}")
