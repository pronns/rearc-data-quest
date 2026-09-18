import sys

sys.path.append("/Workspace/Users/pronnoy1998@gmail.com/rearc-quest/src/alternatives/gold_pyspark_pipeline/transformations")
from decimal import Decimal

import pytest
from gold_pyspark import population_stats, series_best_year
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("tests").getOrCreate()

def test_best_year_matches_readme_example(spark):
    # README: values 1, 2 in 1995 and 3, 4 in 1996 -> best year 1996 with sum 7
    obs = spark.createDataFrame(
        [("PRS30006011", 1995, "Q01", Decimal("1.000")), ("PRS30006011", 1995, "Q02",
          Decimal("2.000")),
         ("PRS30006011", 1996, "Q01", Decimal("3.000")), ("PRS30006011", 1996, "Q02",
          Decimal("4.000")),
         ("PRS30006011", 1996, "Q05", Decimal("99.000"))],  # annual average must be ignored
        "series_id string, year int, period string, value decimal(18,3)")
    dim = spark.createDataFrame([("PRS30006011", "Label")], "series_id string, series_label string") \
        .selectExpr("series_id", "series_label", "null sector_name", "null measure_text",
                   "null class_text", "null duration_text", "null seasonal_text", "null base_year")
    row = series_best_year(obs, dim).collect()[0]
    assert (row.best_year, row.best_year_value) == (1996, Decimal("7.000"))

def test_population_stats_window(spark):
    pop = spark.createDataFrame([(2012, 100), (2013, 100), (2018, 200), (2019, 999)],
                                "year int, population long")
    row = population_stats(pop).collect()[0]
    assert row.n_years == 2 and row.mean_population == 150.0