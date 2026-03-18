
from typing import List, Optional, Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel


class MemoryOperationsMixin:

    persistent_rdds: Optional[List[RDD]] = None

    def __init__(self, persistent_rdds: Optional[List[RDD]] = None,):
        self.persistent_rdds = persistent_rdds or []

    def cleanup(self):
        """
        Un-persists all rdds in persistent_rdds.

        Returns:
            None
        """
        while self.persistent_rdds:
            rdd = self.persistent_rdds.pop()
            rdd.unpersist()

    @staticmethod
    def _get_underlying_rdd(df_or_rdd: Union[DataFrame, "dsp.pipeline.models.DataFrameInfo", RDD]) -> RDD:
        from dsp.pipeline.models import DataFrameInfo
        if isinstance(df_or_rdd, DataFrame):
            return df_or_rdd.rdd
        elif isinstance(df_or_rdd, DataFrameInfo):
            return df_or_rdd.df.rdd
        elif not isinstance(df_or_rdd, RDD):
            raise TypeError(f"Cannot retrieve RDD for object {df_or_rdd}, which is a {type(df_or_rdd)}.")

        return df_or_rdd

    @staticmethod
    def _get_underlying_df(df_or_dfi: Union[DataFrame, "dsp.pipeline.models.DataFrameInfo"]) -> DataFrame:
        from dsp.pipeline.models import DataFrameInfo

        if isinstance(df_or_dfi, DataFrame):
            return df_or_dfi
        elif isinstance(df_or_dfi, DataFrameInfo):
            return df_or_dfi.df
        else:
            raise TypeError(f"Cannot retrieve DataFrame for object {df_or_dfi}, which is a {type(df_or_dfi)}.")

    @staticmethod
    def _get_checkpoint_persistent_rdd(jrdd, spark: SparkSession):
        if jrdd.id() in spark.sparkContext._jsc.getPersistentRDDs():
            return jrdd

        deps = spark.sparkContext._jvm.org.apache.spark.api.java.JavaRDD.toRDD(jrdd).dependencies()

        if deps.size() == 0:
            return None

        return MemoryOperationsMixin._get_checkpoint_persistent_rdd(deps.apply(0).rdd().toJavaRDD(), spark)

    def local_checkpoint(self,
                         df_or_dfi: Union[DataFrame,
                                          "dsp.pipeline.models.DataFrameInfo"],
                         eager=True) -> Optional[DataFrame]:
        df = self._get_underlying_df(df_or_dfi)
        df = df.localCheckpoint(eager=eager)

        persisted_rdd = self._get_checkpoint_persistent_rdd(df.rdd._jrdd, df.sql_ctx.sparkSession)
        if not persisted_rdd:
            raise Exception("Could not get the persisted RDD.")

        self.persistent_rdds.append(persisted_rdd)
        return df

    def checkpoint(self,
                   df_or_dfi: Union[DataFrame,
                                    "dsp.pipeline.models.DataFrameInfo"],
                   eager=True) -> Optional[DataFrame]:
        df = self._get_underlying_df(df_or_dfi)

        # checkpoint does not persist rdd internally

        return df.checkpoint(eager=eager)

    def cache(self, df_or_rdd: Union[DataFrame, "dsp.pipeline.models.DataFrameInfo", RDD]):
        rdd = self._get_underlying_rdd(df_or_rdd)  # type: RDD
        rdd.persist(StorageLevel.MEMORY_AND_DISK)
        self.persistent_rdds.append(rdd)

    def persist(self,
                df_or_rdd: Union[DataFrame,
                                 "dsp.pipeline.models.DataFrameInfo",
                                 RDD],
                storage_level=StorageLevel.MEMORY_AND_DISK):
        rdd = self._get_underlying_rdd(df_or_rdd)  # type: RDD
        rdd.persist(storage_level)
        self.persistent_rdds.append(rdd)
