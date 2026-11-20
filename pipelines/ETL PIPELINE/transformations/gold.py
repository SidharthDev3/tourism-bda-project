
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# =====================================================
# GLOBAL TOURISM INTELLIGENCE
# GOLD LAYER - ANALYTICAL DATASETS
# =====================================================

SOURCE_TABLE = "workspace.default.silver_tourism_etl"


# =====================================================
# 1. COUNTRY-WISE YEARLY TOURISM ANALYSIS
# =====================================================

@dp.materialized_view(
    name="gold_country_yearly",
    comment="Yearly international tourist arrivals by country."
)
def gold_country_yearly():

    df = spark.read.table(SOURCE_TABLE)

    # Keep country-level entities with valid country codes
    df = df.filter(
        F.col("code").isNotNull() &
        (F.trim(F.col("code")) != "")
    )

    result = (
        df.groupBy(
            "entity",
            "code",
            "world_region_according_to_owid",
            "year"
        )
        .agg(
            F.sum(
                "arrivals_of_tourists_from_abroad"
            ).alias("total_tourist_arrivals")
        )
        .orderBy("year", "entity")
    )

    return result


# =====================================================
# 2. REGION-WISE YEARLY TOURISM ANALYSIS
# =====================================================

@dp.materialized_view(
    name="gold_region_yearly",
    comment="Yearly international tourist arrivals aggregated by world region."
)
def gold_region_yearly():

    df = spark.read.table(SOURCE_TABLE)

    # Aggregate only country-level records to avoid
    # double counting existing regional aggregates.
    df = df.filter(
        F.col("code").isNotNull() &
        (F.trim(F.col("code")) != "") &
        F.col("world_region_according_to_owid").isNotNull()
    )

    result = (
        df.groupBy(
            "world_region_according_to_owid",
            "year"
        )
        .agg(
            F.sum(
                "arrivals_of_tourists_from_abroad"
            ).alias("total_tourist_arrivals")
        )
        .orderBy("year", "world_region_according_to_owid")
    )

    return result


# =====================================================
# 3. YEAR-WISE GLOBAL TOURISM ANALYSIS
# =====================================================

@dp.materialized_view(
    name="gold_global_yearly",
    comment="Yearly global tourism arrivals calculated from country-level records."
)
def gold_global_yearly():

    df = spark.read.table(SOURCE_TABLE)

    df = df.filter(
        F.col("code").isNotNull() &
        (F.trim(F.col("code")) != "")
    )

    result = (
        df.groupBy("year")
        .agg(
            F.sum(
                "arrivals_of_tourists_from_abroad"
            ).alias("total_tourist_arrivals"),

            F.countDistinct("code").alias(
                "number_of_countries"
            )
        )
        .orderBy("year")
    )

    return result
