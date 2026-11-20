
import re
from pyspark import pipelines as dp


# ==========================================
# GLOBAL TOURISM INTELLIGENCE
# BRONZE LAYER
# ==========================================

# Source table from the ingestion pipeline
SOURCE_TABLE = (
    "`workspace`.`tourism-schema`."
    "`_1t_s9qkxaoisrwvklja9rfoubccuvb_f5`"
)


# ==========================================
# COLUMN NAME STANDARDIZATION
# ==========================================

def clean_column_names(columns):

    cleaned_columns = []
    used_names = set()

    for column in columns:

        new_name = re.sub(
            r"[^a-zA-Z0-9_]+",
            "_",
            column.strip()
        ).strip("_").lower()

        if not new_name:
            new_name = "column"

        if new_name[0].isdigit():
            new_name = "col_" + new_name

        original_name = new_name
        counter = 1

        while new_name in used_names:
            new_name = f"{original_name}_{counter}"
            counter += 1

        used_names.add(new_name)
        cleaned_columns.append(new_name)

    return cleaned_columns


# ==========================================
# BRONZE MATERIALIZED VIEW
# ==========================================

@dp.materialized_view(
    name="bronze_tourism_etl",
    comment="Raw tourism data ingested from Our World in Data."
)
def bronze_tourism():

    # Read source tourism data
    df = spark.read.table(SOURCE_TABLE)

    # Standardize column names
    new_columns = clean_column_names(df.columns)

    df = df.toDF(*new_columns)

    # Return raw records
    return df
