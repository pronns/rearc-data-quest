-- Silver owns cleaning: trim, type, validate, de-duplicate, conform labels.
CREATE OR REFRESH MATERIALIZED VIEW rearc_quest.silver.silver_pr_observations (
CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL AND series_id <> '') ON
VIOLATION FAIL UPDATE,
CONSTRAINT year_valid EXPECT (year BETWEEN 1940 AND 2100) ON
VIOLATION DROP ROW,
CONSTRAINT period_valid EXPECT (period RLIKE '^Q0[1-5]$') ON
VIOLATION DROP ROW,
CONSTRAINT value_numeric EXPECT (value IS NOT NULL) ON
VIOLATION DROP ROW,
CONSTRAINT series_id_format EXPECT (series_id RLIKE '^PR[SU][0-9]{8}$') ON
VIOLATION DROP ROW
) COMMENT '
One row per (series_id, year, period). Whitespace trimmed, value cast to DECIMAL(18,3),
duplicates across pr.data.* files resolved.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH cleaned AS (
SELECT
trim(series_id) AS series_id,
try_cast(trim(year) AS INT) AS year,
trim(period) AS period,
try_cast(trim(value) AS DECIMAL(18,3)) AS value,
nullif(trim(footnote_codes), '') AS footnote_codes,
_source_file, _file_modified
FROM rearc_quest.bronze.bronze_pr_data
),
ranked AS (
SELECT *,
row_number() OVER (
PARTITION BY series_id, year, period
ORDER BY CASE WHEN _source_file LIKE '%pr.data.0.Current' THEN 0 ELSE 1 END, -- prefer YTD extract
_file_modified DESC
) AS rn
FROM cleaned
) SELECT series_id, year, period, value, footnote_codes,
(period = 'Q05') AS is_annual_average,
regexp_extract(_source_file, '[^/]+$', 0) AS source_file,
_file_modified
FROM ranked
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW rearc_quest.silver.silver_pr_series_dim (
CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT sector_resolved EXPECT (sector_name IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT measure_resolved EXPECT (measure_text IS NOT NULL) ON VIOLATION DROP ROW
) COMMENT '
Conformed series dimension: codes joined to their mapping files and composed into a
human-readable label.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH s AS (
SELECT trim(series_id) AS series_id, trim(sector_code) AS sector_code, trim(class_code) AS
class_code,
trim(measure_code) AS measure_code, trim(duration_code) AS duration_code,
trim(seasonal) AS seasonal_code,
try_cast(trim(base_year) AS INT) AS base_year,
try_cast(trim(begin_year) AS INT) AS begin_year, trim(begin_period) AS begin_period,
try_cast(trim(end_year) AS INT) AS end_year, trim(end_period) AS end_period,
nullif(trim(footnote_codes), '') AS footnote_codes
FROM rearc_quest.bronze.bronze_pr_series
) SELECT
s.series_id, s.sector_code, sec.sector_name, s.measure_code, m.measure_text,
s.class_code, c.class_text, s.duration_code, d.duration_text, s.seasonal_code,
se.seasonal_text,
s.base_year, s.begin_year, s.begin_period, s.end_year, s.end_period, s.footnote_codes,
concat_ws(' ', sec.sector_name, '-', m.measure_text,
concat('(', concat_ws(', ', c.class_text, d.duration_text, se.seasonal_text), ')'))
AS series_label
FROM s
LEFT JOIN (SELECT trim(sector_code) sector_code, trim(sector_name) sector_name FROM
rearc_quest.bronze.bronze_pr_sector) sec USING (sector_code)
LEFT JOIN (SELECT trim(measure_code) measure_code, trim(measure_text) measure_text FROM
rearc_quest.bronze.bronze_pr_measure) m USING (measure_code)
LEFT JOIN (SELECT trim(class_code) class_code, trim(class_text) class_text FROM
rearc_quest.bronze.bronze_pr_class) c USING (class_code)
LEFT JOIN (SELECT trim(duration_code) duration_code, trim(duration_text) duration_text FROM
rearc_quest.bronze.bronze_pr_duration) d USING (duration_code)
LEFT JOIN (SELECT trim(seasonal_code) seasonal_code, trim(seasonal_text) seasonal_text FROM
rearc_quest.bronze.bronze_pr_seasonal) se USING (seasonal_code);

CREATE OR REFRESH MATERIALIZED VIEW rearc_quest.silver.silver_population (
  CONSTRAINT year_not_null       EXPECT (year IS NOT NULL)        ON VIOLATION FAIL UPDATE,
  CONSTRAINT population_positive EXPECT (population > 0)          ON VIOLATION DROP ROW,
  CONSTRAINT single_nation       EXPECT (nation_id = '01000US')   ON VIOLATION DROP ROW
)
COMMENT 'US total population by year (ACS 1-year, via DataUSA). Latest snapshot only.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH r AS (
  SELECT
    try_cast(year_raw AS INT)                              AS year,
    try_cast(
      try_cast(population_raw AS DOUBLE) AS BIGINT
    )                                                      AS population,
    nation,
    nation_id,
    _file_modified
  FROM rearc_quest.bronze.bronze_population
)
SELECT year, population, nation, nation_id,
       _file_modified AS snapshot_modified
FROM (
  SELECT *, row_number() OVER (PARTITION BY year ORDER BY _file_modified DESC) AS rn
  FROM r
)
WHERE rn = 1;