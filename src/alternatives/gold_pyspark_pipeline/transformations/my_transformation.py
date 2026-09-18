# Registers the PySpark versions as pipeline tables with a _py suffix.
from gold_pyspark import population_stats, series_best_year, series_q01_with_population
from pyspark import (
    pipelines as dp,  # on older runtimes: `import dlt as dp` and use dp.table
)


@dp.materialized_view(name="rearc_quest.gold.gold_population_stats_py",
    comment="PySpark alternative for Q1 (parity-tested against SQL).")
@dp.expect("six_years_present", "n_years = 6")
def gold_population_stats_py():
    return population_stats(spark.read.table("rearc_quest.silver.silver_population"))

@dp.materialized_view(name="rearc_quest.gold.gold_series_best_year_py",
    comment="PySpark alternative for Q2.")
@dp.expect_or_fail("series_id_not_null", "series_id IS NOT NULL")
def gold_series_best_year_py():
    return series_best_year(spark.read.table("rearc_quest.silver.silver_pr_observations"),
        spark.read.table("rearc_quest.silver.silver_pr_series_dim"))

@dp.materialized_view(name="rearc_quest.gold.gold_prs30006032_q01_population_py",
    comment="PySpark alternative for Q3.")
def gold_prs30006032_q01_population_py():
    return series_q01_with_population(spark.read.table("rearc_quest.silver.silver_pr_observations"),
        spark.read.table("rearc_quest.silver.silver_pr_series_dim"),
        spark.read.table("rearc_quest.silver.silver_population"))