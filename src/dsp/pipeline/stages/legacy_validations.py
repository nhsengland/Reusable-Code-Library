import os
from typing import Any, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, current_timestamp

from dsp.dam import evaluate_rejection, uniqueness_dq_rules, data_format_dq_rules, data_type_coercions, \
    ds_typed_dq_rules, dq_message_meta_fields, dq_field_values
from dsp.datasets.common import Fields as CommonFields
from dsp.dq_files.dq.schema import DQ_MESSAGE_SCHEMA
from dsp.dq_files.apply_dq_rules import uniqueness_dq, data_dq
from dsp.dq_files.coerce_types import coerce_data_types
from dsp.pipeline import ValidationResult, Metadata, get_ingestion_output_location
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage, PipelineRejection
from dsp.shared.constants import PATHS


class LegacyValidationsStage(PipelineStage):
    """
    Pipieline stage which invokes validations performed by pipeline 1.0. The implementation selects which validations to
    apply by the dataset ID. The use of this stage should be phased out in favour of specific validation phases,
    configured for the dataset registered to the pipeline.
    """

    name = "legacy_validations"

    def __init__(self, input_dataframe_name: str = "df", output_dataframe_name: str = "df"):
        """
        Args:
            input_dataframe_name (str): Name of the dataframe input to this stage to validate; default value "df"
            output_dataframe_name (str): Name to give to the dataframe output by this stage; default value "df"
        """
        super().__init__(name=LegacyValidationsStage.name, required_input_dataframes={input_dataframe_name},
                         provided_output_dataframes={output_dataframe_name})
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    @staticmethod
    def should_reject(validation_result: ValidationResult, metadata: Metadata) -> bool:
        dataset_id = metadata['dataset_id']
        return evaluate_rejection(dataset_id)(validation_result)

    @staticmethod
    def validate(raw_df: DataFrame, metadata: Metadata, loader_validation_result: ValidationResult) \
            -> Tuple[DataFrame, ValidationResult]:

        dataset_id = metadata['dataset_id']

        output_folder = get_ingestion_output_location(metadata)

        # uniqueness check - nb. no rows removed prior to validation check
        dq_uniqueness_error_df, dq_uniqueness_out_df = uniqueness_dq(raw_df, uniqueness_dq_rules(dataset_id),
                                                                     dq_field_values(dataset_id))

        # run dq rules to check data formats and discard any failures
        dq_errors_untyped_df, dq_warnings_untyped_df, dq_pass_df = data_dq(
            dq_uniqueness_out_df, data_format_dq_rules(dataset_id), dq_field_values(dataset_id),
            dq_message_meta_fields(dataset_id)
        )

        coercions = data_type_coercions(dataset_id)

        # convert to useful types where possible
        typed_df = coerce_data_types(dq_pass_df, coercions)

        # run dq rules to dataset validity now typing has been applied
        dq_errors_typed_df, dq_warnings_typed_df, dq_pass_typed_df = data_dq(typed_df, ds_typed_dq_rules(dataset_id),
                                                                             dq_field_values(dataset_id),
                                                                             dq_message_meta_fields(dataset_id)
                                                                             )

        # union the errors
        dq_errors_df = dq_errors_typed_df.union(dq_errors_untyped_df).union(dq_uniqueness_error_df)

        # Cache ahead of using the dataframe in various ways
        dq_errors_df.cache()

        errs_empty = dq_errors_df.rdd.isEmpty()

        # union the warnings
        dq_warnings_df = dq_warnings_typed_df.union(dq_warnings_untyped_df)

        # Cache ahead of using the dataframe in various ways
        dq_warnings_df.cache()

        warns_empty = dq_warnings_df.rdd.isEmpty()

        if errs_empty and warns_empty:
            dq_errors_df.unpersist()
            dq_warnings_df.unpersist()
            return dq_pass_typed_df, loader_validation_result

        dq_results_path = os.path.join(output_folder, PATHS.DQ)

        def expand_dq(dq_df: DataFrame) -> DataFrame:
            return dq_df.withColumn(CommonFields.DQ, explode(CommonFields.DQ)) \
                .select(*[col(CommonFields.DQ + "." + field.name).alias(field.name) for field in DQ_MESSAGE_SCHEMA]) \
                .withColumn(CommonFields.DQ_TS, current_timestamp())

        records_with_dq_errors = 0
        if not errs_empty:
            expand_dq(dq_errors_df).write.mode("append").parquet(dq_results_path)
            records_with_dq_errors = dq_errors_df.count()

        dq_errors_df.unpersist()

        records_with_dq_warnings = 0
        if not warns_empty:
            expand_dq(dq_warnings_df).write.mode("append").parquet(dq_results_path)
            records_with_dq_warnings = dq_warnings_df.count() - records_with_dq_errors

        dq_warnings_df.unpersist()

        loader_validation_result.dq_error_count += records_with_dq_errors
        loader_validation_result.dq_warning_count += records_with_dq_warnings

        return dq_pass_typed_df, loader_validation_result

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any,
             loader_validation_result: ValidationResult = None, **kwargs: Any) -> PipelineContext:

        data_frame = context.dataframes[self._input_dataframe_name].df

        if not loader_validation_result:
            loader_validation_result = ValidationResult(
                total_records=data_frame.count(),
                dq_error_count=0,
                dq_warning_count=0
            )

        validated_df, validation_result = self.validate(data_frame, context.to_metadata(), loader_validation_result)

        if self.should_reject(validation_result, context.to_metadata()):
            raise PipelineRejection

        new_context = context.clone()
        new_context.dataframes = {self._output_dataframe_name: DataFrameInfo(validated_df)}
        return new_context
