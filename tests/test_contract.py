"""
Data contract test: asserts pipeline table schemas match contracts/bls_pr.yml
Run via: pytest tests/test_contract.py (locally with PySpark)
Or run as a notebook in Databricks after pipeline completes.
"""
import pytest
import yaml
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("contract-tests") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture(scope="session")
def contract():
    with open("contracts/bls_pr.yml") as f:
        return yaml.safe_load(f)

def test_bronze_row_counts(spark, contract):
    """Every Bronze table meets its minimum row count."""
    mins = contract["bronze_expectations"]["min_row_counts"]
    for table, min_count in mins.items():
        count = spark.table(f"rearc_quest.bronze.{table}").count()
        assert count >= min_count, \
            f"{table}: expected >= {min_count} rows, got {count}"

def test_silver_no_duplicates(spark, contract):
    """Silver tables have no duplicate keys."""
    for table, spec in contract["silver_expectations"].items():
        keys = spec["key_columns"]
        df = spark.table(f"rearc_quest.silver.{table}")
        total = df.count()
        distinct = df.select(keys).distinct().count()
        assert total == distinct, \
            f"{table}: {total - distinct} duplicate keys on {keys}"

def test_silver_row_counts(spark, contract):
    """Silver tables are within expected row count bounds."""
    for table, spec in contract["silver_expectations"].items():
        count = spark.table(f"rearc_quest.silver.{table}").count()
        if "min_rows" in spec:
            assert count >= spec["min_rows"], \
                f"{table}: expected >= {spec['min_rows']} rows, got {count}"
        if "max_rows" in spec:
            assert count <= spec["max_rows"], \
                f"{table}: expected <= {spec['max_rows']} rows, got {count}"

def test_gold_population_stats_values(spark):
    """Q1 answer matches expected values from verified API payload."""
    row = spark.table("rearc_quest.gold.gold_population_stats").collect()[0]
    assert row.n_years == 6, f"Expected 6 years, got {row.n_years}"
    assert abs(float(row.mean_population) - 322069808.0) < 1.0, \
        f"Mean population mismatch: {row.mean_population}"
    assert abs(float(row.stddev_sample) - 4158441.04) < 100.0, \
        f"Stddev mismatch: {row.stddev_sample}"

def test_gold_series_best_year_has_labels(spark):
    """Every row in best_year table has a human-readable label."""
    df = spark.table("rearc_quest.gold.gold_series_best_year")
    nulls = df.where("series_label IS NULL").count()
    assert nulls == 0, f"{nulls} series have no label"

def test_gold_q3_has_population(spark):
    """Q3 table has population for years where API has data."""
    df = spark.table("rearc_quest.gold.gold_prs30006032_q01_population")
    # 2013-2019, 2021-2024 should have population (not 2020)
    years_with_pop = df.where(
        "population IS NOT NULL AND year BETWEEN 2013 AND 2024 AND year != 2020"
    ).count()
    assert years_with_pop >= 10, \
        f"Expected >= 10 years with population, got {years_with_pop}"