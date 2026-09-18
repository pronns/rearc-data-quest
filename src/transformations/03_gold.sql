-- Q1: mean and standard deviation of annual US population, 2013-2018 inclusive.
CREATE OR REFRESH MATERIALIZED VIEW rearc_quest.gold.gold_population_stats (
CONSTRAINT six_years_present EXPECT (n_years = 6) -- warn only: visible in pipeline metrics, does not block
) COMMENT '
Answer to Q1. stddev_sample uses n-1 (Spark default stddev); stddev_population uses n.'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
2013 AS start_year,
2018 AS end_year,
count(*) AS n_years,
min(year) AS first_year_found,
max(year) AS last_year_found,
round(avg(population), 2) AS mean_population,
round(stddev_samp(population), 2) AS stddev_sample,
round(stddev_pop(population), 2) AS stddev_population
FROM rearc_quest.silver.silver_population
WHERE year BETWEEN 2013 AND 2018;
-- Q2: for every series_id, the year with the largest sum of value across its quarters (Q01-Q04).
-- Q05 is the BLS annual average (pr.txt Section 7), not a quarter, so it is excluded.
CREATE OR REFRESH MATERIALIZED VIEW rearc_quest.gold.gold_series_best_year (
CONSTRAINT one_row_per_series EXPECT (series_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT label_present EXPECT (series_label IS NOT NULL)
) COMMENT '
Answer to Q2. best_year = year with max sum(value) over Q01..Q04; ties broken by latest
year. is_complete_year flags 4 quarters present.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH yearly AS (
SELECT series_id, year,
sum(value) AS annual_sum,
count(*) AS n_quarters
FROM rearc_quest.silver.silver_pr_observations
WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
GROUP BY series_id, year
),
ranked AS (
SELECT *, row_number() OVER (PARTITION BY series_id ORDER BY annual_sum DESC, year DESC) AS rn
FROM yearly
) SELECT
r.series_id,
coalesce(d.series_label, r.series_id) AS series_label,
d.sector_name, d.measure_text, d.class_text, d.duration_text, d.seasonal_text, d.base_year,
r.year AS best_year,
r.annual_sum AS best_year_value,
r.n_quarters,
(r.n_quarters = 4) AS is_complete_year
FROM ranked r
LEFT JOIN rearc_quest.silver.silver_pr_series_dim d USING (series_id)
WHERE r.rn = 1;
-- Q3: series PRS30006032, period Q01, value per year, joined with population where available.
CREATE OR REFRESH MATERIALIZED VIEW rearc_quest.gold.gold_prs30006032_q01_population (
CONSTRAINT year_not_null EXPECT (year IS NOT NULL) ON VIOLATION FAIL UPDATE
) COMMENT '
Answer to Q3. LEFT JOIN keeps every BLS year; population is NULL where the API has no
figure (e.g. before 2013, 2020).'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
o.series_id,
d.series_label,
o.year,
o.period,
o.value,
p.population
FROM rearc_quest.silver.silver_pr_observations o
LEFT JOIN rearc_quest.silver.silver_population p ON o.year = p.year
LEFT JOIN rearc_quest.silver.silver_pr_series_dim d ON o.series_id = d.series_id
WHERE o.series_id = 'PRS30006032' AND o.period = 'Q01';