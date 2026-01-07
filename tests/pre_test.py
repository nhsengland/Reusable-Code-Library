# Databricks notebook source
# MAGIC %md
# MAGIC To be run before each test notebook i.e. in its own cell `%run ./pre_test`
# MAGIC and the `pre_test` function called

# COMMAND ----------

try:
    dbutils
except NameError:
    import pyspark
    from delta import configure_spark_with_delta_pip
    #from plics_integrated_2324.staging.plics_integrated_2324.tests.dev_static_database import dev_protect_databases
    
    class LocalSpark:
        """Context manager / encapsulation class for singleton spark object"""

        spark = None

        def __init__(self, metastore_dir="/tmp/metastore"):
            warehouse_dir = f"{metastore_dir}/warehouse"
            derby_dir = f"{metastore_dir}/derby"

            builder = (
                pyspark.sql.SparkSession.builder.appName("MyApp")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", f"{warehouse_dir}")
                .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
                .enableHiveSupport()
            )

            LocalSpark.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        @classmethod
        def clean_metastore(cls): #, dev_protect_databases: set = dev_protect_databases):
            databases = cls.spark.sql("SHOW DATABASES").select("namespace").collect()
            for db_row in databases:
                db_name = db_row.namespace
                
                # if db_name in dev_protect_databases:
                #     continue
                
                if db_name != "default":
                    cls.spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
                    continue
                
                # clean but don't remove default dabase
                tables = cls.spark.sql(f"SHOW TABLES IN {db_name}").select("tableName").collect()
                for table in tables:
                    cls.spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table.tableName}")

        @classmethod
        def backup_metastore(cls):
            backup = []

            databases = cls.spark.sql("SHOW DATABASES").select("namespace").collect()
            for db_row in databases:
                db_name = db_row.namespace
                tables = cls.spark.sql(f"SHOW TABLES IN {db_name}").select("tableName").collect()
                table_info = []
                for table in tables:
                    table_name = table.tableName
                    table_df = cls.spark.table(f"{db_name}.{table_name}")
                    table_schema = table_df.schema
                    table_data = table_df.collect()
                    table_info.append({"name": table_name, "schema": table_schema, "data": table_data})

                backup.append({"db_name": db_name, "tables": table_info})

            return backup

        @classmethod
        def restore_metastore(cls, db_list):
            for db_entry in db_list:
                db_name = db_entry["db_name"]
                cls.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                cls.spark.sql(f"USE {db_name}")
                for table_info in db_entry["tables"]:
                    table_name = table_info["name"]
                    table_schema = table_info["schema"]
                    table_data = table_info["data"]
                    cls.spark.catalog.createTable(table_name, schema=table_schema)
                    cls.spark.createDataFrame(table_data, schema=table_schema).write.insertInto(table_name)

        @classmethod
        def clean_database(cls, db_name=None):
            if db_name:
                cls.spark.sql(f"USE {db_name}")
            table_names = [row[1] for row in cls.spark.sql("SHOW TABLES").collect()]
            for table_name in table_names:
                cls.spark.sql(f"DELETE FROM {table_name}")


    spark = LocalSpark().spark
    
    def display(df):
        """Since display is better than df.show() in databricks, using display by default and faking it in local environment."""
        df.show(truncate=False)


# COMMAND ----------


# Defined as a function and them ran inline in a given test,
# so that the widgets are create in the test notebook.
def pre_test():
    try:
        dbutils
        dbutils.widgets.text("db", "testdata_plics_integrated_plics_integrated", "TARGET DATABASE")
        db = dbutils.widgets.get("db")

    except NameError:
        print("Running outside of Databricks Environment: Pre_Test skipping setting of db Name.")
        return

    if not db:
        raise TypeError("db is required")

    spark.sql(f"USE {db}")
