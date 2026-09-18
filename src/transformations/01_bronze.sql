-- ============================================================
-- 01_bronze.sql
-- One materialized view per BLS file + population API.
-- All columns STRING. Trim applied here to handle BLS padding.
-- ============================================================

-- ----------------------------------------------------------
-- FACT: pr.data.* (both Current and AllData)
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_data (
  CONSTRAINT series_id_present EXPECT (series_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT year_present       EXPECT (year IS NOT NULL)      ON VIOLATION FAIL UPDATE,
  CONSTRAINT period_present     EXPECT (period IS NOT NULL)    ON VIOLATION FAIL UPDATE
)
COMMENT 'BLS PR data files as received. Dedup happens in Silver.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(_c0) AS series_id,
  trim(_c1) AS year,
  trim(_c2) AS period,
  trim(_c3) AS value,
  trim(_c4) AS footnote_codes,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _file_modified,
  current_timestamp()              AS _ingested_at
FROM read_files(
  '${raw_root}/bls/pr/pr.data.*',
  format => 'csv',
  sep => '\t',
  header => false,
  inferSchema => false
)
WHERE trim(_c0) != 'series_id';

-- ----------------------------------------------------------
-- DIMENSION: pr.series
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_series (
  CONSTRAINT series_id_present EXPECT (series_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'pr.series: one row per series with component codes and begin/end dates.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(_c0)  AS series_id,
  trim(_c1)  AS sector_code,
  trim(_c2)  AS class_code,
  trim(_c3)  AS measure_code,
  trim(_c4)  AS duration_code,
  trim(_c5)  AS seasonal,
  trim(_c6)  AS base_year,
  trim(_c7)  AS footnote_codes,
  trim(_c8)  AS begin_year,
  trim(_c9)  AS begin_period,
  trim(_c10) AS end_year,
  trim(_c11) AS end_period,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _file_modified,
  current_timestamp()              AS _ingested_at
FROM read_files(
  '${raw_root}/bls/pr/pr.series',
  format => 'csv',
  sep => '\t',
  header => false,
  inferSchema => false
)
WHERE trim(_c0) != 'series_id';

-- ----------------------------------------------------------
-- MAPPING: pr.sector
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_sector
COMMENT 'pr.sector mapping'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(sector_code)  AS sector_code,
  trim(sector_name)  AS sector_name,
  trim(display_level) AS display_level,
  trim(selectable)   AS selectable,
  trim(sort_sequence) AS sort_sequence,
  _rescued_data,
  _metadata.file_modification_time AS _file_modified
FROM read_files(
  '${raw_root}/bls/pr/pr.sector',
  format => 'csv',
  sep => '\t',
  header => true,
  inferSchema => false,
  schema => 'sector_code STRING, sector_name STRING, display_level STRING,
             selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ----------------------------------------------------------
-- MAPPING: pr.measure
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_measure
COMMENT 'pr.measure mapping'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(measure_code) AS measure_code,
  trim(measure_text) AS measure_text,
  trim(display_level) AS display_level,
  trim(selectable)   AS selectable,
  trim(sort_sequence) AS sort_sequence,
  _rescued_data,
  _metadata.file_modification_time AS _file_modified
FROM read_files(
  '${raw_root}/bls/pr/pr.measure',
  format => 'csv',
  sep => '\t',
  header => true,
  inferSchema => false,
  schema => 'measure_code STRING, measure_text STRING, display_level STRING,
             selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ----------------------------------------------------------
-- MAPPING: pr.class
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_class
COMMENT 'pr.class mapping'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(class_code)   AS class_code,
  trim(class_text)   AS class_text,
  trim(display_level) AS display_level,
  trim(selectable)   AS selectable,
  trim(sort_sequence) AS sort_sequence,
  _rescued_data,
  _metadata.file_modification_time AS _file_modified
FROM read_files(
  '${raw_root}/bls/pr/pr.class',
  format => 'csv',
  sep => '\t',
  header => true,
  inferSchema => false,
  schema => 'class_code STRING, class_text STRING, display_level STRING,
             selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ----------------------------------------------------------
-- MAPPING: pr.duration
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_duration
COMMENT 'pr.duration mapping'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(duration_code) AS duration_code,
  trim(duration_text) AS duration_text,
  trim(display_level) AS display_level,
  trim(selectable)    AS selectable,
  trim(sort_sequence) AS sort_sequence,
  _rescued_data,
  _metadata.file_modification_time AS _file_modified
FROM read_files(
  '${raw_root}/bls/pr/pr.duration',
  format => 'csv',
  sep => '\t',
  header => true,
  inferSchema => false,
  schema => 'duration_code STRING, duration_text STRING, display_level STRING,
             selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ----------------------------------------------------------
-- MAPPING: pr.seasonal
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_seasonal
COMMENT 'pr.seasonal mapping. BLS uses capitalised headers (Seasonal_code, Seasonal_text).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(`Seasonal_code`) AS seasonal_code,
  trim(`Seasonal_text`) AS seasonal_text,
  _rescued_data,
  _metadata.file_modification_time AS _file_modified
FROM read_files(
  '${raw_root}/bls/pr/pr.seasonal',
  format => 'csv',
  sep => '\t',
  header => true,
  inferSchema => false,
  schema => '`Seasonal_code` STRING, `Seasonal_text` STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ----------------------------------------------------------
-- MAPPING: pr.period
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_period
COMMENT 'pr.period mapping (Q05 = Annual Average)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(period)       AS period,
  trim(period_abbr)  AS period_abbr,
  trim(period_name)  AS period_name,
  _rescued_data,
  _metadata.file_modification_time AS _file_modified
FROM read_files(
  '${raw_root}/bls/pr/pr.period',
  format => 'csv',
  sep => '\t',
  header => true,
  inferSchema => false,
  schema => 'period STRING, period_abbr STRING, period_name STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ----------------------------------------------------------
-- MAPPING: pr.footnote
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_pr_footnote
COMMENT 'pr.footnote mapping'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  trim(footnote_code) AS footnote_code,
  trim(footnote_text) AS footnote_text,
  _rescued_data,
  _metadata.file_modification_time AS _file_modified
FROM read_files(
  '${raw_root}/bls/pr/pr.footnote',
  format => 'csv',
  sep => '\t',
  header => true,
  inferSchema => false,
  schema => 'footnote_code STRING, footnote_text STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ----------------------------------------------------------
-- POPULATION: DataUSA API (latest snapshot)
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_population (
  CONSTRAINT has_records EXPECT (nation_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'DataUSA population API, one row per year. Nation ID space handled via get_json_object.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  get_json_object(rec, '$.Year')         AS year_raw,
  get_json_object(rec, '$.Population')   AS population_raw,
  get_json_object(rec, '$.Nation')       AS nation,
  get_json_object(rec, '$["Nation ID"]') AS nation_id,
  _metadata.file_path                    AS _source_file,
  _metadata.file_modification_time       AS _file_modified,
  current_timestamp()                    AS _ingested_at
FROM (
  SELECT
    explode(from_json(
      get_json_object(value, '$.data'),
      'array<string>'
    )) AS rec,
    _metadata
  FROM read_files(
    '${raw_root}/population/latest/population.json',
    format => 'text',
    wholeText => true
  )
);