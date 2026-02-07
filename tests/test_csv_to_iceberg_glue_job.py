import pytest
from pyspark.sql import SparkSession
from glue_jobs.csv_to_iceberg_glue_job import (
    validate_csv_path,
    detect_header,
    read_csv_dynamic,
    cleanse_dataframe,
    add_ingestion_metadata,
)

# -------------------------------------------------
# SPARK SESSION FIXTURE
# -------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark")
        .getOrCreate()
    )
    yield spark
    spark.stop()

# -------------------------------------------------
# validate_csv_path
# -------------------------------------------------

def test_validate_csv_path_valid():
    validate_csv_path("s3://bucket/data.csv")

def test_validate_csv_path_invalid():
    with pytest.raises(ValueError):
        validate_csv_path("s3://bucket/data.parquet")

# -------------------------------------------------
# detect_header
# -------------------------------------------------

def test_detect_header_true():
    assert detect_header("id,name,email") is True

def test_detect_header_false():
    assert detect_header("1,2,3") is False

# -------------------------------------------------
# read_csv_dynamic
# -------------------------------------------------

def test_read_csv_dynamic_variable_columns(spark, tmp_path):
    csv_data = """a,b
1,2,3
4
"""

    file_path = tmp_path / "test.csv"
    file_path.write_text(csv_data)

    df = read_csv_dynamic(spark, str(file_path))

    assert df.count() == 2
    assert len(df.columns) == 3
    assert df.columns == ["a", "b", "col_3"]

    rows = df.collect()
    assert rows[1]["col_2"] == "0"
    assert rows[1]["col_3"] == "0"

# -------------------------------------------------
# cleanse_dataframe
# -------------------------------------------------

def test_cleanse_dataframe_removes_duplicates_and_trims(spark):
    df = spark.createDataFrame(
        [
            (" 1 ", " abc "),
            (" 1 ", " abc "),
        ],
        ["col1", "col2"],
    )

    cleaned = cleanse_dataframe(df)

    assert cleaned.count() == 1
    row = cleaned.collect()[0]
    assert row.col1 == "1"
    assert row.col2 == "abc"

# -------------------------------------------------
# add_ingestion_metadata
# -------------------------------------------------

def test_add_ingestion_metadata_columns_added(spark):
    df = spark.createDataFrame([("1",)], ["col1"])

    enriched = add_ingestion_metadata(df, "s3://bucket/file.csv")

    assert "ingestion_id" in enriched.columns
    assert "ingestion_ts" in enriched.columns
    assert "source_file" in enriched.columns

    row = enriched.collect()[0]
    assert row.source_file == "s3://bucket/file.csv"