from typing import List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import max

from src.nhs_reusable_code_library.resuable_codes.shared.file_store import File

INGESTION_CONTROL_TABLE = "__ingestion_control"


class IngestionStatus:
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"
    DELETED = "DELETED"


class IngestionController:
    def __init__(
        self,
        spark: SparkSession,
        db: str,
        error_tolerance: int,
        control_table: Optional[str] = INGESTION_CONTROL_TABLE,
    ):
        self._spark = spark
        self.db = db
        self.error_tolerance = error_tolerance
        self.control_table = control_table

        self._create_control_table()

    def _create_control_table(self):
        self._spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.db}.{self.control_table} (
                file_id BIGINT,
                file_path STRING,
                file_version STRING,
                ingestion_time TIMESTAMP,
                status STRING,
                status_information STRING
            ) USING delta PARTITIONED BY (status)
            """
        )

    def verify_ingestion_status(self) -> None:
        failed_files = self.list_failed_files()
        if len(failed_files) > self.error_tolerance:
            file_ids = [row.file_id for row in failed_files]
            message = (
                "Cannot proceed with file ingestion. "
                f"Unrecoverable errors exist for file ids: {file_ids}. "
                "Please resolve these manually before retrying."
            )

            raise ValueError(message)

    def list_pending_files(self) -> List[Tuple[int, str, str]]:
        return self._list_files(IngestionStatus.PENDING)

    def list_failed_files(self) -> List[Tuple[int, str, str, str]]:
        return self._list_files(IngestionStatus.ERROR)

    def list_skipped_files(self) -> List[Tuple[int, str, str, str]]:
        return self._list_files(IngestionStatus.SKIPPED)

    def skip_file(self, file: File) -> int:
        file_id = self._get_next_file_id()
        status = IngestionStatus.SKIPPED
        status_information = "File did not pass ingestion pre-check."

        self._spark.sql(
            f"""INSERT INTO {self.db}.{self.control_table}
            VALUES ({file_id}, '{self._escape_string(file.absolute_path)}',
            '{file.version}', CURRENT_TIMESTAMP(), '{status}',
            '{status_information}')"""
        )

        return file_id

    def start_file_ingestion(self, file: File) -> int:
        file_id = self._get_next_file_id()
        status = IngestionStatus.PENDING
        status_information = ""

        self._spark.sql(
            f"""INSERT INTO {self.db}.{self.control_table}
            VALUES ({file_id}, '{self._escape_string(file.absolute_path)}',
            '{file.version}', CURRENT_TIMESTAMP(), '{status}',
            '{status_information}')"""
        )

        return file_id

    def start_file_retry(self, file_id: int) -> None:
        self.set_file_ingestion_pending(file_id)

    def set_file_ingestion_complete(self, file_id: int) -> None:
        self._update_file_status(file_id, IngestionStatus.COMPLETE)

    def set_file_ingestion_pending(self, file_id: int) -> None:
        self._update_file_status(file_id, IngestionStatus.PENDING)

    def set_file_ingestion_error(self, file_id: int, info: str) -> None:
        self._update_file_status(file_id, IngestionStatus.ERROR, info)
        print(f"File ingestion failed with error: {info}")
        self.verify_ingestion_status()

    # Does not use _update_file_status to avoid overwriting status_information.
    def set_file_deleted(self, file_id: int) -> None:
        self._spark.sql(
            f"""
            UPDATE {self.db}.{self.control_table}
            SET status = '{IngestionStatus.DELETED}'
            WHERE file_id = {file_id} AND status <> '{IngestionStatus.DELETED}'
            """
        )

    def _update_file_status(
        self, file_id: int, status: str, info: Optional[str] = ""
    ) -> None:
        self._spark.sql(
            f"""
            UPDATE {self.db}.{self.control_table} SET
            status = '{status}', status_information = '{self._escape_string(info)}'
            WHERE file_id = {file_id} AND status <> '{status}'
            """
        )

    def _list_files(self, status: str) -> List[Tuple[int, str, str, str]]:
        control_df = self._spark.table(f"{self.db}.{self.control_table}")
        return (
            control_df.select(
                control_df.file_id, control_df.file_path, control_df.file_version, control_df.status_information
            )
            .where(control_df.status == status)
            .orderBy(control_df.file_id)
            .collect()
        )

    def _get_next_file_id(self) -> int:
        control_df = self._spark.table(f"{self.db}.{self.control_table}")
        last_id = (
            control_df.select(max(control_df.file_id).alias("file_id")).head().file_id
        )

        return 1 if last_id is None else last_id + 1

    @staticmethod
    def _escape_string(string: str) -> str:
        return string.replace("\\", "\\\\").replace("'", "\\'")
