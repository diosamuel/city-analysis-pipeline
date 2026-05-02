-- ============================================================
-- STAR SCHEMA DATA WAREHOUSE
-- Smart City: Traffic, Weather & Air Quality Analysis
-- ============================================================
-- Single fact table.
-- Grain   : camera_code × date × time
-- Assumption: each camera inherits the nearest weather & AQI
--             readings matched by location during ETL.
-- ============================================================

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- dim_date ────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS dwh_seq_dim_date START 1;

CREATE TABLE IF NOT EXISTS dim_date (
  date_sk        BIGINT PRIMARY KEY DEFAULT nextval('dwh_seq_dim_date'),
  full_date      DATE NOT NULL UNIQUE,
  year           SMALLINT NOT NULL,
  quarter        SMALLINT NOT NULL,
  month          SMALLINT NOT NULL,
  month_name     VARCHAR(16) NOT NULL,
  week_of_year   SMALLINT NOT NULL,
  day_of_month   SMALLINT NOT NULL,
  day_of_week    SMALLINT NOT NULL,
  day_name       VARCHAR(16) NOT NULL,
  is_weekend     BOOLEAN NOT NULL,
  is_holiday     BOOLEAN NOT NULL DEFAULT FALSE
);

-- dim_time (hour + minute resolution) ────────────────────────
CREATE SEQUENCE IF NOT EXISTS dwh_seq_dim_time START 1;

CREATE TABLE IF NOT EXISTS dim_time (
  time_sk    BIGINT PRIMARY KEY DEFAULT nextval('dwh_seq_dim_time'),
  full_time  TIME NOT NULL UNIQUE,
  hour       SMALLINT NOT NULL,
  minute     SMALLINT NOT NULL,
  period     VARCHAR(2) NOT NULL,
  time_band  VARCHAR(32) NOT NULL
);

-- dim_location ───────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS dwh_seq_dim_location START 1;

CREATE TABLE IF NOT EXISTS dim_location (
  location_sk   BIGINT PRIMARY KEY DEFAULT nextval('dwh_seq_dim_location'),
  adm1_code     VARCHAR(16),
  adm2_code     VARCHAR(16),
  adm3_code     VARCHAR(16),
  adm4_code     VARCHAR(32),
  provinsi      VARCHAR(128),
  kotkab        VARCHAR(128),
  kecamatan     VARCHAR(128),
  kelurahan     VARCHAR(128),
  latitude      DOUBLE,
  longitude     DOUBLE,
  elevation     FLOAT DEFAULT 0,
  timezone      VARCHAR(32),
  luas          DOUBLE,
  penduduk      BIGINT
);

CREATE INDEX IF NOT EXISTS idx_dim_loc_adm2   ON dim_location (adm2_code);
CREATE INDEX IF NOT EXISTS idx_dim_loc_adm3   ON dim_location (adm3_code);
CREATE INDEX IF NOT EXISTS idx_dim_loc_adm4   ON dim_location (adm4_code);
CREATE INDEX IF NOT EXISTS idx_dim_loc_coords ON dim_location (latitude, longitude);

-- dim_camera (business key = camera_code, bridged to location)
CREATE TABLE IF NOT EXISTS dim_camera (
  camera_code   VARCHAR(32) PRIMARY KEY,
  location_sk   BIGINT NOT NULL,
  latitude      DOUBLE,
  longitude     DOUBLE,
  location_text TEXT,
  route_slug    VARCHAR(64),
  adm2_code     VARCHAR(13),
  adm4_code     VARCHAR(13)
);

-- dim_aqi_category ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_aqi_category (
  aqi_category_sk SMALLINT PRIMARY KEY,
  category_code   VARCHAR(32) NOT NULL UNIQUE,
  category_name   VARCHAR(64) NOT NULL,
  min_value       INTEGER,
  max_value       INTEGER
);

INSERT INTO dim_aqi_category VALUES
  (1, 'baik',                 'Baik',                                  0,   50),
  (2, 'sedang',               'Sedang',                               51,  100),
  (3, 'tidak_sehat_sensitif', 'Tidak Sehat bagi Kelompok Sensitif',  101,  150),
  (4, 'tidak_sehat',          'Tidak Sehat',                         151,  200),
  (5, 'sangat_tidak_sehat',   'Sangat Tidak Sehat',                  201,  300),
  (6, 'berbahaya',            'Berbahaya',                           301, NULL);

-- dim_aqi_station ─────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS dwh_seq_dim_aqi_station START 1;

CREATE TABLE IF NOT EXISTS dim_aqi_station (
  aqi_station_sk BIGINT PRIMARY KEY DEFAULT nextval('dwh_seq_dim_aqi_station'),
  location_sk    BIGINT NOT NULL,
  id_stasiun     VARCHAR(128),
  nama           VARCHAR(256),
  alamat         TEXT,
  tipe           VARCHAR(8),
  tipe_text      VARCHAR(32),
  stasiun_uji    VARCHAR(8),
  p3e            VARCHAR(64)
);

-- ============================================================
-- SINGLE FACT TABLE
-- ============================================================
-- Grain : camera_code × date_sk × time_sk
--
-- Traffic speed & volume columns are pivoted by direction and
-- vehicle type so everything sits in one row per camera-moment.
-- Weather & AQI are pre-matched to each camera by location
-- during the ETL load.
-- ============================================================
CREATE SEQUENCE IF NOT EXISTS dwh_seq_fact_smart_city START 1;

CREATE TABLE IF NOT EXISTS fact_smart_city (
  fact_sk              BIGINT PRIMARY KEY DEFAULT nextval('dwh_seq_fact_smart_city'),

  -- ── keys ──────────────────────────────────────────────────
  camera_code          VARCHAR(32) NOT NULL REFERENCES dim_camera (camera_code),
  location_sk          BIGINT NOT NULL REFERENCES dim_location (location_sk),
  date_sk              BIGINT NOT NULL REFERENCES dim_date (date_sk),
  time_sk              BIGINT NOT NULL REFERENCES dim_time (time_sk),

  -- ── traffic speed (source: vehicle_speed) ─────────────────
  speed_normal         NUMERIC(10,3),
  speed_opposite       NUMERIC(10,3),
  speed_gol_normal     NUMERIC(10,3),
  speed_gol_opposite   NUMERIC(10,3),
  speed_max_gol_normal   NUMERIC(10,3),
  speed_max_gol_opposite NUMERIC(10,3),
  vehicle_count_normal   INTEGER,
  vehicle_count_opposite INTEGER,

  -- ── hourly volume by vehicle type (source: hourly_vehicle_speed)
  volume_sm            NUMERIC(14,3),
  volume_mp            NUMERIC(14,3),
  volume_ks            NUMERIC(14,3),
  volume_bb            NUMERIC(14,3),
  volume_tb            NUMERIC(14,3),
  total_volume_sm      NUMERIC(14,3),
  total_volume_mp      NUMERIC(14,3),
  total_volume_ks      NUMERIC(14,3),
  total_volume_bb      NUMERIC(14,3),
  total_volume_tb      NUMERIC(14,3),

  -- ── weather (source: bmkg_weather, matched by location) ───
  temperature          NUMERIC(5,2),
  temperature_min      NUMERIC(5,2),
  temperature_max      NUMERIC(5,2),
  humidity             SMALLINT,
  humidity_min         SMALLINT,
  humidity_max         SMALLINT,
  weather_code         SMALLINT,
  weather_description  VARCHAR(128),
  wind_speed           NUMERIC(6,2),
  wind_direction       VARCHAR(8),
  visibility_text      VARCHAR(64),

  -- ── air quality (source: air_quality, matched by location) ─
  aqi_station_sk       BIGINT REFERENCES dim_aqi_station (aqi_station_sk),
  aqi_category_sk      SMALLINT REFERENCES dim_aqi_category (aqi_category_sk),
  aqi_value            INTEGER,
  aqi_dominant_param   VARCHAR(128),
  pm10_actual          NUMERIC(10,2),
  pm25_actual          NUMERIC(10,2),
  so2_actual           NUMERIC(10,2),
  co_actual            NUMERIC(10,2),
  o3_actual            NUMERIC(10,2),
  no2_actual           NUMERIC(10,2),
  hc_actual            NUMERIC(10,2),
  pm10_ispu            SMALLINT,
  pm25_ispu            SMALLINT,
  so2_ispu             SMALLINT,
  co_ispu              SMALLINT,
  o3_ispu              SMALLINT,
  no2_ispu             SMALLINT,
  hc_ispu              SMALLINT,
  pm10_critical        INTEGER,
  pm25_critical        INTEGER,
  so2_critical         INTEGER,
  co_critical          INTEGER,
  o3_critical          INTEGER,
  no2_critical         INTEGER,
  hc_critical          INTEGER,

  -- ── metadata ──────────────────────────────────────────────
  ingested_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── indexes ─────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_camera   ON fact_smart_city (camera_code);
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_smart_city (location_sk);
CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_smart_city (date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_time     ON fact_smart_city (time_sk);
