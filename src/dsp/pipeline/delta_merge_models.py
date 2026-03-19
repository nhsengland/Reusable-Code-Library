from typing import Sequence


class DeltaMergeParameters:

    def __init__(self, dataset_id: str, source_directory: str, target_table: str, join_columns: Sequence[str],
                 output_columns: Sequence[str]):
        self.dataset_id = dataset_id
        self.source_directory = source_directory
        self.target_table = target_table
        self.join_columns = tuple(join_columns)
        self.output_columns = tuple(output_columns)
