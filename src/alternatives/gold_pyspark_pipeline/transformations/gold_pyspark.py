"""PySpark implementations of the three Gold questions.
Documented alternative to 03_gold.sql. Each function is a pure transformation."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

QUARTERS = ["Q01", "Q02", "Q03", "Q04"] # Q05 = annual average (pr.txt Section 7), excluded

def population_stats(pop: DataFrame, start: int = 2013, end: int = 2018) -> DataFrame:
    """Q1: mean and standard deviation of annual population across [start, end]."""
    return (
            pop.where(F.col("year").between(start, end))
            .agg(
            F.lit(start).alias("start_year"),
            F.lit(end).alias("end_year"),
            F.count("*").alias("n_years"),
            F.min("year").alias("first_year_found"),
            F.max("year").alias("last_year_found"),
            F.round(F.avg("population"), 2).alias("mean_population"),
            F.round(F.stddev_samp("population"), 2).alias("stddev_sample"),
            F.round(F.stddev_pop("population"), 2).alias("stddev_population"),
            )
    )

def series_best_year(obs: DataFrame, dim: DataFrame) -> DataFrame:
    """Q2: per series_id, the year with the largest sum(value) over quarterly periods."""
    yearly = (
        obs.where(F.col("period").isin(QUARTERS))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("annual_sum"), F.count("*").alias("n_quarters"))
    )
    w = Window.partitionBy("series_id").orderBy(F.col("annual_sum").desc(),F.col("year").desc())
    best = yearly.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
    return (
    best.alias("r")
    .join(dim.alias("d"), on="series_id", how="left")
    .select(
    "series_id",
    F.coalesce(F.col("d.series_label"), F.col("series_id")).alias("series_label"),
    "d.sector_name", "d.measure_text", "d.class_text", "d.duration_text",
    "d.seasonal_text", "d.base_year",
    F.col("r.year").alias("best_year"),
    F.col("r.annual_sum").alias("best_year_value"),
    "r.n_quarters",
    (F.col("r.n_quarters") == 4).alias("is_complete_year"),
    )
    )

def series_q01_with_population(obs: DataFrame, dim: DataFrame, pop: DataFrame,
                               series_id: str = "PRS30006032", period: str = "Q01") -> DataFrame:
    """Q3: one series/period per year, left-joined to population."""
    return (
        obs.where((F.col("series_id") == series_id) & (F.col("period") == period)).alias("o")
        .join(pop.select("year", "population").alias("p"), on="year", how="left")
        .join(dim.select("series_id", "series_label").alias("d"), on="series_id", how="left")
        .select("o.series_id", "d.series_label", "year", "o.period", "o.value",
                "p.population")
    )