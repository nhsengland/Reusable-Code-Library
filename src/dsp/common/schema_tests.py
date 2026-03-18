# pylint: disable=redefined-outer-name,expression-not-assigned,pointless-statement
# pylint: disable=unused-import,protected-access,invalid-name
"""Tests for the Schema."""
import json
from typing import Iterator, List
from uuid import uuid4

import pytest  # type: ignore
from pyspark.sql.functions import col  # pylint: disable=no-name-in-module
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dsp.common.schema import Schema
from dsp.validations.conftest import spark, temp_db


@pytest.fixture(scope="function")
def fields() -> Iterator[List[StructField]]:
    """Sample constructor fields."""
    yield [
        StructField("Name", StringType()),
        StructField("Age", IntegerType()),
        StructField("DoB", DateType()),
        StructField("AddressLines", ArrayType(StringType(), False)),
        StructField(
            "META",
            StructType(
                [
                    StructField("Record_Index", IntegerType()),
                    StructField("Record_Version", IntegerType()),
                    StructField("Dataset_Version", IntegerType()),
                    StructField("Event_Received_TS", TimestampType()),
                    StructField("Event_ID", StringType()),
                ]
            ),
        ),
    ]


def test_len_iter(fields: List[StructField]):
    """Test that len/iter are as expected."""
    schema = Schema(fields)
    assert len(schema) == len(fields)
    assert list(schema) == fields


def test_constructor_catches_field_issues():
    """Test that Type/Value errors are raised appropriately."""
    with pytest.raises(TypeError):
        Schema(["A string!"])

    with pytest.raises(ValueError):
        Schema([StructField("Name", StringType()), StructField("Name", StringType())])


def test_fields_match_structtype(fields: List[StructField]):
    """Ensure that using the normal constructor matches a `StructType`."""
    schema = Schema(fields)
    struct = StructType(fields)

    # Though the nested `StructType` will be a `Schema` too since it's
    # applied recursively.
    assert schema.fields == struct.fields
    assert schema.names == struct.names


def test_json_interop_with_structtype(fields: List[StructField]):
    """Ensure that the JSON representation matches a `StructType`."""
    schema = Schema(fields)
    struct = StructType(fields)
    schema_json = schema.json()
    struct_json = struct.json()

    assert schema_json == struct_json

    assert schema.from_dict(json.loads(struct_json)) == struct
    assert schema == struct.fromJson(json.loads(schema_json))

    assert schema.from_json_string(struct_json) == struct
    assert schema == struct.fromJson(schema.to_dict())
    # `to_json_string` is pretty printed by default.
    assert schema == struct.fromJson(json.loads(schema.to_json_string()))


def test_schema_from_structtype(fields: List[StructField]):
    """Test that a `Schema` can be created from a `StructType`."""
    struct = StructType(fields)
    assert Schema(struct) == struct


def test_sorting_schema(fields: List[StructField]):
    """Test that a schema can be sorted."""
    schema = Schema(fields)
    sorted_schema = schema.sorted()
    assert schema.fields != sorted_schema.fields
    assert sorted(schema.names) == sorted_schema.names

    # Check nested structs.
    nested_before = schema.get_nested("META")

    # When the sort includes nested structs:
    nested_after = sorted_schema.get_nested("META")
    assert nested_before.fields != nested_after.fields
    assert sorted(nested_before.names) == nested_after.names

    # When it doesn't:
    sorted_not_nested = schema.sorted(sort_nested=False)
    nested_after = sorted_not_nested.get_nested("META")
    assert schema.fields != sorted_schema.fields
    assert nested_before.fields == nested_after.fields


def test_schema_getitem(fields: List[StructField]):
    """Test that __getitem__'s behaviour is as expected."""
    schema = Schema(fields)

    # Lookup by name
    name_field = schema["Name"]
    assert isinstance(name_field, StructField)
    assert name_field.name == "Name"

    # Lookup nested struct name
    meta_event_id_field = schema["META.Event_ID"]
    assert isinstance(meta_event_id_field, StructField)
    assert meta_event_id_field.name == "Event_ID"

    # Lookup ints/slices
    assert schema[0] == fields[0]
    assert schema[0:2] == fields[0:2]

    # Lookup tuple of names.
    struct_list = schema["Name", "Age"]
    assert isinstance(struct_list, list)
    assert len(struct_list) == 2
    assert all(
        isinstance(item, StructField) for item in struct_list  # pylint: disable=E1133
    )
    assert [item.name for item in struct_list] == ["Name", "Age"]  # pylint: disable=E1133
    # Lookup tuple of ints.
    indices = (schema.names.index("Name"), schema.names.index("Age"))
    assert schema[indices] == struct_list

    # Create new schema from list of names.
    new_schema = schema[["Name", "Age", "META.Event_ID"]]
    assert isinstance(new_schema, Schema)
    assert len(new_schema) == 3
    assert new_schema.names == ["Name", "Age", "Event_ID"]

    # Test various failure conditions.
    with pytest.raises(KeyError):
        schema["Not_In_Schema"]

    with pytest.raises(IndexError):
        schema[len(fields) + 1]

    with pytest.raises(TypeError):
        # Can't mix ints/strings.
        schema[1, "String"]  # type: ignore

    with pytest.raises(TypeError):
        # Can't use sets of items.
        schema[{"Name", "Age"}]  # type: ignore

    with pytest.raises(TypeError):
        schema[[None]]  # type: ignore


def test_schema_get(fields: List[StructField]):
    """Test that get's behaviour is as expected."""
    schema = Schema(fields)
    assert isinstance(schema.get("Name"), StructField)
    assert isinstance(schema.get("META"), StructField)
    assert isinstance(schema.get("META.Event_ID"), StructField)
    assert isinstance(schema.get(1), StructField)

    with pytest.raises(IndexError):
        schema.get(len(fields) + 1)

    with pytest.raises(KeyError):
        schema.get("Not_In_Schema")

    with pytest.raises(TypeError):
        schema.get((1, 2))  # type: ignore


def test_schema_get_nested(fields: List[StructField]):
    """Test that get_nested's behaviour is as expected."""
    schema = Schema(fields)
    assert isinstance(schema.get_nested("META"), Schema)
    assert isinstance(schema.get_nested(-1), Schema)

    with pytest.raises(KeyError):
        schema.get_nested("Not_In_Schema")

    with pytest.raises(IndexError):
        schema.get_nested(len(fields) + 1)

    with pytest.raises(TypeError):
        schema.get_nested([1, 2])  # type: ignore

    with pytest.raises(ValueError):
        schema.get_nested("Name")


def test_normalisation(fields: List[StructField]):
    """Test that the normalise method works as expected."""
    schema = Schema(fields)
    upper_field_names = [field.name.upper() for field in fields]
    assert schema.normalise().names == upper_field_names
    lower_field_names = [field.name.lower() for field in fields]
    assert schema.normalise(str.lower).names == lower_field_names

    meta_field: StructField = next(filter(lambda f: f.name == "META", fields))
    meta_type: StructType = meta_field.dataType  # type: ignore
    meta_field_names = [field.name.upper() for field in meta_type]
    meta_struct = schema.get_nested("META")
    assert meta_struct.normalise().names == meta_field_names  # type: ignore


def test_schema_from_keys(fields: List[StructField]):
    """Test the `Schema.from_keys` constructor."""
    keys = {field.name: field for field in fields}
    assert Schema(fields).names == Schema.from_keys(**keys).names


def test_schema_contains():
    """Test that 'contains' works for unnested and nested levels."""
    third_level = Schema.from_keys(Level3=StringType())
    second_level = Schema.from_keys(Level2=third_level)
    first_level = Schema.from_keys(Level1=second_level, NotALevel=StringType())
    assert "Level3" in third_level
    assert "Level2.Level3" in second_level
    assert "Level1.Level2.Level3" in first_level

    assert "Level3.something" not in third_level
    assert "MissingLevel.something" not in third_level
    assert "Level2.notthere" not in second_level
    assert "NotALevel.something" not in first_level

    with pytest.raises(TypeError):
        assert 3 in third_level


def test_schema_add_drop(fields: List[StructField]):
    """Test that schema manipulation works as expected and is immutable."""
    schema = Schema(fields)
    new_schema = schema.add("Another_Field", StringType())

    # Ensure that 'Another_Field' is not in `schema.`
    assert schema != new_schema
    with pytest.raises(KeyError):
        schema["Another_Field"]

    new_schema = schema.drop("Name")
    assert "Name" in schema
    assert "Name" not in new_schema

    with pytest.raises(ValueError):
        # Need DataType if just string is passed.
        schema.add("Another_Field")

    new_schema = schema.add("IntegerField", "integer")
    assert isinstance(new_schema["IntegerField"].dataType, IntegerType)  # type: ignore

    new_schema = schema.add("IntegerField", StructField("discarded", IntegerType()))
    assert isinstance(new_schema["IntegerField"].dataType, IntegerType)  # type: ignore

    new_schema = schema.drop("META.Event_ID")
    assert new_schema.get_nested("META") == schema.get_nested("META").drop("Event_ID")


def test_schema_rename(fields: List[StructField]):
    """Test that renaming elements works as expected."""
    schema = Schema(fields)
    new_schema = schema.rename(Name="NAME")
    # Equality checks are case insensitive
    assert new_schema == schema

    new_schema = schema.rename(Name="Namez")  # Like 'name' but cooler.
    assert new_schema != schema

    new_schema = schema.rename(**{"META": "Something", "META.Event_ID": "EVENT"})
    assert "Something" in new_schema and "Something.EVENT" in new_schema

    with pytest.raises(ValueError):
        new_schema.rename(**{"META.Event_ID": "Something.Something"})

    with pytest.raises(ValueError):
        new_schema.rename(**{"Name.Name": "Another"})


def test_schema_replace(fields: List[StructField]):
    """Test that replacing elements works as expected."""
    schema = Schema(fields)
    # Someone is really old.
    new_schema = schema.replace("Age", LongType())
    assert schema != new_schema

    # And with a StructField.
    another_new_schema = schema.replace(StructField("Age", LongType()))
    assert new_schema == another_new_schema

    with pytest.raises(KeyError):
        schema.replace("NotInSchema", IntegerType())


def test_select(fields: List[StructField]):
    """
    Test that select works. This is like getitem but always returns the
    root schema.

    """
    schema = Schema(fields)
    assert schema.select("Name", "Age").names == ["Name", "Age"]
    assert schema.select("META").names == ["META"]


def test_df_validate(spark: SparkSession):
    """Test that a DataFrame can be validated."""
    schema = Schema.from_keys(Age=LongType(), Name=StringType())

    rows = [
        Row(Name="Travis", Age=24),
        Row(Name="David", Age=95),
        Row(Name="Zainab", Age=54),
    ]

    df = spark.createDataFrame(rows)
    assert schema.validate(df)


def test_schema_can_create_table(spark: SparkSession, temp_db: str):
    """Test that a table can be created from a schema."""
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    schema.create_as_table(spark, temp_db, "test_table")
    assert schema == spark.table(f"`{temp_db}`.test_table").schema


def test_schema_can_create_table_auto_optimize(spark: SparkSession, temp_db: str):
    """
    Test that a table can be created from a schema with auto-optimisation
    enabled.

    """
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    schema.create_as_table(
        spark, temp_db, "test_table", auto_compact=True, auto_optimize=True
    )
    assert schema == spark.table(f"`{temp_db}`.test_table").schema

    prop_options = (
        spark.sql(f"SHOW TBLPROPERTIES {temp_db}.test_table")
        .filter(col("key").rlike("delta\\.autoOptimize\\..+"))
        .collect()
    )
    prop_dict = {row["key"]: row["value"] for row in prop_options}
    assert prop_dict == {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }


def test_schema_auto_optimize_warns_non_delta(spark: SparkSession, temp_db: str):
    """Test that trying to enable 'auto-optimize' on a non-delta table raises."""
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    with pytest.warns(UserWarning):
        schema.create_as_table(
            spark,
            temp_db,
            "test_table",
            table_format="orc",
            auto_compact=True,
            auto_optimize=True,
        )

    assert schema == spark.table(f"`{temp_db}`.test_table").schema

    prop_options = (
        spark.sql(f"SHOW TBLPROPERTIES {temp_db}.test_table")
        .filter(col("key").rlike("delta\\.autoOptimize\\..+"))
        .collect()
    )
    prop_dict = {row["key"]: row["value"] for row in prop_options}
    assert prop_dict == {}


def test_schema_create_table_no_db(spark: SparkSession):
    """
    Test that trying to create a table raises an exception if the database
    does not exist.

    """
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    with pytest.raises(ValueError):
        schema.create_as_table(spark, uuid4().hex, "test_table")


def test_schema_create_table_no_table_name(spark: SparkSession):
    """
    Test that trying to create a table raises an exception if the table name
    is not provided.

    """
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    with pytest.raises(ValueError):
        schema.create_as_table(spark, uuid4().hex)


def test_schema_can_not_raise_if_table_exists(spark: SparkSession, temp_db: str):
    """
    Test that an error can be ignored if a table already exists.

    """
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    schema.create_as_table(spark, temp_db, "test_table")
    schema.create_as_table(spark, temp_db, "test_table")


def test_schema_can_raise_if_table_exists(spark: SparkSession, temp_db: str):
    """
    Test that an error can be raised if a table already exists and we try to
    create it.

    """
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    schema.create_as_table(spark, temp_db, "test_table")
    with pytest.raises(ValueError):
        schema.create_as_table(spark, temp_db, "test_table", exist_ok=False)


def test_create_table_error_handling(spark: SparkSession, temp_db: str):
    """
    Test that create table error handling works as expected.

    """
    schema = Schema.from_keys(Name=StringType(), Age=IntegerType())
    schema.create_as_table(spark, temp_db, "test_table", table_format="parquet")

    with pytest.raises(ValueError):
        schema.add("ID", StringType()).create_as_table(
            spark, temp_db, "test_table", table_format="parquet"
        )


def test_schema_raises_incorrect_format(spark: SparkSession, temp_db: str):
    """
    Test that an error is raised if the existing table is in the wrong format.

    """
    schema = Schema.from_keys(Age=IntegerType(), Location=StringType())

    schema.create_as_table(spark, temp_db, "test_table")
    with pytest.raises(ValueError):
        schema.create_as_table(spark, temp_db, "test_table", table_format="parquet")


def test_schema_coersion(spark: SparkSession):
    """Test that a DataFrame can be coerced into a new schema."""
    schema = Schema.from_keys(Identifier=StringType())
    df = spark.createDataFrame([["1"], ["2"], ["22"], ["975"]], schema)  # type: ignore

    new_schema = Schema.from_keys(Identifier=IntegerType())
    new_df = new_schema.coerce(df)
    values = [row[0] for row in new_df.collect()]
    assert values == [1, 2, 22, 975]


def test_schema_coersion_missing_column(spark: SparkSession):
    """Test that a DataFrame can be coerced into a new schema with a new column."""
    schema = Schema.from_keys(Identifier=StringType())
    df = spark.createDataFrame([["1"], ["2"], ["22"], ["975"]], schema)  # type: ignore

    new_schema = Schema.from_keys(Identifier=IntegerType(), Name=StringType())
    new_df = new_schema.coerce(df)
    values = [(row["Identifier"], row["Name"]) for row in new_df.collect()]
    assert values == [(1, None), (2, None), (22, None), (975, None)]


def test_schema_coersion_missing_column_not_nullable(spark: SparkSession):
    """
    Test that a DataFrame cannot be coerced into a new schema with a new column
    if that column is nullable.

    """
    schema = Schema.from_keys(Identifier=StringType())
    df = spark.createDataFrame([["1"], ["2"], ["22"], ["975"]], schema)  # type: ignore

    new_schema = schema.add("Name", StringType(), False)
    with pytest.raises(ValueError):
        new_schema.coerce(df)


def test_schema_coersion_column_made_nullable(spark: SparkSession):
    """
    Test that if a previously nullable column was made non-nullable, nulls
    are dropped.

    """
    schema = Schema.from_keys(Identifier=StringType())
    df = spark.createDataFrame([["1"], ["2"], ["22"], [None]], schema)  # type: ignore

    new_schema = Schema([StructField("Identifier", StringType(), False)])
    with pytest.warns(UserWarning):
        new_df = new_schema.coerce(df)
    values = [row[0] for row in new_df.collect()]
    assert values == ["1", "2", "22"]


def test_create_df_with_schema(spark: SparkSession):
    """Test that a DataFrame can be created using a Schema."""
    schema = Schema.from_keys(Age=IntegerType(), Name=StringType())
    rows = [Row(Name="Travis", Age=24)]
    spark.createDataFrame(rows, schema)  # type: ignore


def test_addition():
    """Test that schemas can be added."""
    schema1 = Schema.from_keys(Age=IntegerType())
    schema2 = Schema.from_keys(Name=StringType())

    result = Schema.from_keys(Age=IntegerType(), Name=StringType())
    assert schema1 + schema2 == result
    assert schema2 + schema1 == result.select("Name", "Age")

    # And checking that it works with StructTypes.
    struct1 = StructType([StructField("Age", IntegerType())])
    assert struct1 + schema2 == result
    assert schema2 + struct1 == result.select("Name", "Age")


def test_set_union():
    """Test that schema set union works."""
    schema1 = Schema.from_keys(Age=IntegerType(), Location=StringType())
    schema2 = Schema.from_keys(Age=IntegerType(), Name=StringType())

    assert set((schema1 | schema2).names) == {"Age", "Name", "Location"}
    assert set((schema2 | schema1).names) == {"Age", "Name", "Location"}

    # And checking that it works with StructTypes.
    struct1 = StructType(
        [StructField("Age", IntegerType()), StructField("Location", StringType())]
    )
    assert set((struct1 | schema2).names) == {"Age", "Name", "Location"}
    assert set((schema2 | struct1).names) == {"Age", "Name", "Location"}


def test_set_and():
    """Test that schema set and works."""
    schema1 = Schema.from_keys(Age=IntegerType(), Location=StringType())
    schema2 = Schema.from_keys(Age=IntegerType(), Name=StringType())

    result = Schema.from_keys(Age=IntegerType())
    assert schema1 & schema2 == result
    assert schema2 & schema1 == result

    # And checking that it works with StructTypes.
    struct1 = StructType(
        [StructField("Age", IntegerType()), StructField("Location", StringType())]
    )
    assert struct1 & schema2 == result
    assert schema2 & struct1 == result


def test_set_sub():
    """Test that set substitution works."""
    schema1 = Schema.from_keys(Age=IntegerType(), Location=StringType())
    schema2 = Schema.from_keys(Age=IntegerType(), Name=StringType())

    result = Schema.from_keys(Location=StringType(), Name=StringType())
    assert schema1 - schema2 == result.select("Location")
    assert schema2 - schema1 == result.select("Name")

    # And checking that it works with StructTypes.
    struct1 = StructType(
        [StructField("Age", IntegerType()), StructField("Location", StringType())]
    )
    assert struct1 - schema2 == result.select("Location")
    assert schema2 - struct1 == result.select("Name")
