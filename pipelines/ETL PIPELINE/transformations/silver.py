
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# =====================================================
# GLOBAL TOURISM INTELLIGENCE
# SILVER LAYER - DATA CLEANING AND VALIDATION
# =====================================================

SOURCE_TABLE = "workspace.default.bronze_tourism_etl"


@dp.materialized_view(
    name="silver_tourism_etl",
    comment="Cleaned and validated global tourism data."
)
def silver_tourism_etl():

    # -------------------------------------------------
    # 1. READ BRONZE DATA
    # -------------------------------------------------

    df = spark.read.table(SOURCE_TABLE)

    # -------------------------------------------------
    # 2. SELECT ONLY REQUIRED TOURISM COLUMNS
    # Excludes Google Drive connector metadata
    # -------------------------------------------------

    df = df.select(
        "entity",
        "code",
        "year",
        "arrivals_of_tourists_from_abroad",
        "world_region_according_to_owid"
    )

    # -------------------------------------------------
    # 3. CLEAN TEXT COLUMNS
    # -------------------------------------------------

    text_columns = [
        "entity",
        "code",
        "world_region_according_to_owid"
    ]

    for column in text_columns:

        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(column)))
        )

    # -------------------------------------------------
    # 4. CONVERT DATA TYPES
    # -------------------------------------------------

    df = (
        df
        .withColumn(
            "year",
            F.col("year").cast("integer")
        )
        .withColumn(
            "arrivals_of_tourists_from_abroad",
            F.col("arrivals_of_tourists_from_abroad").cast("double")
        )
    )

    # -------------------------------------------------
    # 5. REMOVE INVALID RECORDS
    # -------------------------------------------------

    df = df.filter(
        F.col("entity").isNotNull() &
        (F.col("entity") != "") &
        F.col("year").isNotNull() &
        (F.col("year") > 0) &
        F.col("arrivals_of_tourists_from_abroad").isNotNull() &
        (F.col("arrivals_of_tourists_from_abroad") >= 0)
    )

    # -------------------------------------------------
    # 6. REMOVE DUPLICATE RECORDS
    # Only tourism columns are included
    # -------------------------------------------------

    df = df.dropDuplicates([
        "entity",
        "code",
        "year",
        "arrivals_of_tourists_from_abroad",
        "world_region_according_to_owid"
    ])

    # -------------------------------------------------
    # 7. RETURN CLEANED DATA
    # -------------------------------------------------

    return df
