-- DWH star schema from schema/schema.dbdiagram
-- Grain (fact): one row per camera_sk × calendar_date.
-- air_quality / weather: join or enrich via adm4_sk + time; not enforced as FKs where keys are non-unique.

-- --- dim_date: date_sk = YYYYMMDD integer ---
CREATE TABLE IF NOT EXISTS dim_date (
  date_sk        INTEGER PRIMARY KEY,
  calendar_date  DATE NOT NULL UNIQUE,
  day            INTEGER NOT NULL,
  month          INTEGER NOT NULL,
  year           INTEGER NOT NULL
);

-- --- dim_station (air quality station registry) ---
CREATE TABLE IF NOT EXISTS dim_station (
  station_id   VARCHAR(128) PRIMARY KEY,
  station_lat  DOUBLE,
  station_lon  DOUBLE,
  kota         VARCHAR(128),
  kecamatan    VARCHAR(128)
);

-- --- dim_air_quality: one row per station observation time ---
CREATE TABLE IF NOT EXISTS dim_air_quality (
  station_id             VARCHAR(128) NOT NULL,
  observed_at             TIMESTAMP NOT NULL,
  adm4                    INTEGER,
  aqi_value              INTEGER,
  aqi_category           VARCHAR(64),
  dominant_params_json   TEXT,
  PRIMARY KEY (station_id, observed_at),
  FOREIGN KEY (station_id) REFERENCES dim_station (station_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_air_quality_adm4
  ON dim_air_quality (adm4);

-- --- dim_weather_location ---
CREATE TABLE IF NOT EXISTS dim_weather_location (
  weather_location_sk INTEGER PRIMARY KEY,
  adm4_sk             VARCHAR(13),
  provinsi            VARCHAR(128),
  kotkab              VARCHAR(128),
  kecamatan           VARCHAR(128),
  desa                VARCHAR(128),
  lat                 DOUBLE,
  long                DOUBLE
);

-- --- dim_weather ---
CREATE TABLE IF NOT EXISTS dim_weather (
  weather_location_sk    INTEGER NOT NULL,
  forecast_datetime_utc  TIMESTAMP NOT NULL,
  adm4_sk                VARCHAR(13),
  lat                    DOUBLE,
  lon                    DOUBLE,
  provinsi               VARCHAR(128),
  kotkab                 VARCHAR(128),
  kecamatan              VARCHAR(128),
  temperature_c          INTEGER,
  weather_code           INTEGER,
  weather_desc_en        VARCHAR(128),
  wind_speed_kmh         DOUBLE,
  humidity_pct           INTEGER,
  PRIMARY KEY (weather_location_sk, forecast_datetime_utc),
  FOREIGN KEY (weather_location_sk) REFERENCES dim_weather_location (weather_location_sk)
);

CREATE INDEX IF NOT EXISTS idx_dim_weather_adm4_time
  ON dim_weather (adm4_sk, forecast_datetime_utc);

-- --- dim_camera ---
CREATE SEQUENCE IF NOT EXISTS dwh_seq_dim_camera START 1;

CREATE TABLE IF NOT EXISTS dim_camera (
  camera_sk     BIGINT PRIMARY KEY DEFAULT nextval('dwh_seq_dim_camera'),
  camera_code   VARCHAR(32) NOT NULL UNIQUE,
  latitude      DOUBLE,
  longitude     DOUBLE,
  route_slug    VARCHAR(64),
  adm2          VARCHAR(13),
  adm4_sk       VARCHAR(13),
  location_kode VARCHAR(32),
  jalan         VARCHAR(256),
  kabupaten     VARCHAR(128)
);

-- --- fact: daily CCTV snapshot ---
CREATE SEQUENCE IF NOT EXISTS dwh_seq_fact_cctv_daily START 1;

CREATE TABLE IF NOT EXISTS fact_cctv_daily_snapshot (
  fact_sk                  BIGINT PRIMARY KEY DEFAULT nextval('dwh_seq_fact_cctv_daily'),
  camera_sk                BIGINT NOT NULL,
  adm4_sk                  VARCHAR(13),
  date_sk                  INTEGER NOT NULL,

  avg_speed_kmh            DOUBLE,
  max_speed_kmh            DOUBLE,
  min_speed_kmh            DOUBLE,
  obs_speed_sample_count   BIGINT,

  total_volume_sm          DOUBLE,
  total_volume_mp          DOUBLE,
  total_volume_ks          DOUBLE,
  total_volume_bb          DOUBLE,
  total_volume_tb          DOUBLE,

  speed_timeseries_note    VARCHAR,

  ingest_batch_ts          TIMESTAMP,
  dbt_updated_at           TIMESTAMP,

  UNIQUE (camera_sk, date_sk),
  FOREIGN KEY (camera_sk) REFERENCES dim_camera (camera_sk),
  FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_cctv_daily_date
  ON fact_cctv_daily_snapshot (date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_cctv_daily_adm4
  ON fact_cctv_daily_snapshot (adm4_sk);
