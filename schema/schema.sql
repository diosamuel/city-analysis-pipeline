CREATE TABLE IF NOT EXISTS wilayah_kecamatan_kelurahan (
  kode VARCHAR(13) PRIMARY KEY,
  nama VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXISTS wilayah_name_idx ON wilayah_kecamatan_kelurahan (nama);

CREATE TABLE IF NOT EXISTS wilayah_provinsi_kabupaten (
  kode VARCHAR(13) PRIMARY KEY NOT NULL,
  nama VARCHAR(100),
  ibukota VARCHAR(100),
  lat DOUBLE,
  lng DOUBLE,
  elv FLOAT NOT NULL DEFAULT 0,
  tz SMALLINT,
  luas DOUBLE,
  penduduk BIGINT,
  paths TEXT,
  status SMALLINT
);

-- CCTV camera list

CREATE TABLE IF NOT EXISTS cctv_list (
  camera_code VARCHAR(32) UNIQUE,
  latitude DOUBLE,
  longitude DOUBLE,
  location_text TEXT,
  route_slug VARCHAR(64),
  adm2 VARCHAR(13),
  adm4 VARCHAR(13),
);

-- CCTV chart data
CREATE SEQUENCE IF NOT EXISTS seq_vehicle_speed_id START 1;

CREATE TABLE IF NOT EXISTS vehicle_speed (
  id BIGINT PRIMARY KEY DEFAULT nextval('seq_vehicle_speed_id'),
  camera_code VARCHAR(32),
  last_update_5minutes TIMESTAMP,
  label5min_data JSON,
  listchart1_normal JSON,
  listchart1_opposite JSON,
  speed_normal JSON,
  speed_opposite JSON,
  speed_gol_normal JSON,
  speed_gol_opposite JSON,
  speed_max_gol_normal_list JSON,
  speed_max_gol_opposite_list JSON,
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS seq_hourly_vehicle_speed_id START 1;

CREATE TABLE IF NOT EXISTS hourly_vehicle_speed (
  id BIGINT PRIMARY KEY DEFAULT nextval('seq_hourly_vehicle_speed_id'),
  camera_code VARCHAR(32),
  hourly_label JSON,
  listdata60_60_sm JSON,
  listdata60_60_mp JSON,
  listdata60_60_ks JSON,
  listdata60_60_bb JSON,
  listdata60_60_tb JSON,
  totalvolume_sm NUMERIC(14,3),
  totalvolume_mp NUMERIC(14,3),
  totalvolume_ks NUMERIC(14,3),
  totalvolume_bb NUMERIC(14,3),
  totalvolume_tb NUMERIC(14,3),
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS seq_all_timeseries_vehicle_speed_id START 1;

CREATE TABLE IF NOT EXISTS all_timeseries_vehicle_speed (
  id BIGINT PRIMARY KEY DEFAULT nextval('seq_all_timeseries_vehicle_speed_id'),
  camera_code VARCHAR(32),
  hourly_label JSON,
  datanormal_sm JSON,
  datanormal_mp JSON,
  datanormal_ks JSON,
  datanormal_bb JSON,
  datanormal_tb JSON,
  datanormal_all JSON,
  datalastdatenormal_sm JSON,
  datalastdatenormal_mp JSON,
  datalastdatenormal_ks JSON,
  datalastdatenormal_bb JSON,
  datalastdatenormal_tb JSON,
  datalastdatenormal_all JSON,
  datalast7normal_sm JSON,
  datalast7normal_mp JSON,
  datalast7normal_ks JSON,
  datalast7normal_bb JSON,
  datalast7normal_tb JSON,
  datalast7normal_all JSON,
  dataopposite_sm JSON,
  dataopposite_mp JSON,
  dataopposite_ks JSON,
  dataopposite_bb JSON,
  dataopposite_tb JSON,
  dataopposite_all JSON,
  datalastdateopposite_sm JSON,
  datalastdateopposite_mp JSON,
  datalastdateopposite_ks JSON,
  datalastdateopposite_bb JSON,
  datalastdateopposite_tb JSON,
  datalastdateopposite_all JSON,
  datalast7opposite_sm JSON,
  datalast7opposite_mp JSON,
  datalast7opposite_ks JSON,
  datalast7opposite_bb JSON,
  datalast7opposite_tb JSON,
  datalast7opposite_all JSON,
  databoth_sm JSON,
  databoth_mp JSON,
  databoth_ks JSON,
  databoth_bb JSON,
  databoth_tb JSON,
  databoth_all JSON,
  datalastdateboth_sm JSON,
  datalastdateboth_mp JSON,
  datalastdateboth_ks JSON,
  datalastdateboth_bb JSON,
  datalastdateboth_tb JSON,
  datalastdateboth_all JSON,
  datalast7both_sm JSON,
  datalast7both_mp JSON,
  datalast7both_ks JSON,
  datalast7both_bb JSON,
  datalast7both_tb JSON,
  datalast7both_all JSON,
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- BMKG weather
CREATE SEQUENCE IF NOT EXISTS seq_bmkg_weather_id START 1;

CREATE TABLE IF NOT EXISTS bmkg_weather (
  id BIGINT PRIMARY KEY DEFAULT nextval('seq_bmkg_weather_id'),
  adm1 VARCHAR(16),
  adm2 VARCHAR(16),
  adm3 VARCHAR(16),
  adm4 VARCHAR(32),
  provinsi VARCHAR(128),
  kotkab VARCHAR(128),
  kecamatan VARCHAR(128),
  desa VARCHAR(128),
  lon DOUBLE,
  lat DOUBLE,
  timezone VARCHAR(32),
  weather_data JSON,
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_weather_kecamatan ON bmkg_weather (adm3);
CREATE INDEX IF NOT EXISTS idx_weather_coords ON bmkg_weather (lat, lon);

-- Air Quality Index (KemenLH ISPU)
-- Source: https://ispu.kemenlh.go.id
CREATE SEQUENCE IF NOT EXISTS seq_air_quality_id START 1;

CREATE TABLE IF NOT EXISTS air_quality (
  id BIGINT PRIMARY KEY DEFAULT nextval('seq_air_quality_id'),
  id_stasiun VARCHAR(128),
  waktu TIMESTAMP,
  lat DOUBLE,
  lon DOUBLE,
  a_pm10 NUMERIC(10,2),
  a_pm25 NUMERIC(10,2),
  a_so2 NUMERIC(10,2),
  a_co NUMERIC(10,2),
  a_o3 NUMERIC(10,2),
  a_no2 NUMERIC(10,2),
  a_hc NUMERIC(10,2),
  c_pm10 SMALLINT,
  c_pm25 SMALLINT,
  c_so2 SMALLINT,
  c_co SMALLINT,
  c_o3 SMALLINT,
  c_no2 SMALLINT,
  c_hc SMALLINT,
  t_pm10 INTEGER,
  t_pm25 INTEGER,
  t_so2 INTEGER,
  t_co INTEGER,
  t_o3 INTEGER,
  t_no2 INTEGER,
  t_hc INTEGER,
  nama VARCHAR(256),
  alamat TEXT,
  kota VARCHAR(128),
  provinsi VARCHAR(128),
  param VARCHAR(128),
  stasiun_uji VARCHAR(8),
  stasiun_show VARCHAR(8),
  stasiun_show_detail VARCHAR(8),
  p3e VARCHAR(64),
  time_offset VARCHAR(8),
  time_z VARCHAR(8),
  is_maintenance VARCHAR(8),
  auto_validation VARCHAR(8),
  tipe VARCHAR(8),
  kelurahan VARCHAR(128),
  kecamatan VARCHAR(128),
  val INTEGER,
  cat VARCHAR(64),
  kategori JSON,
  waktu_text VARCHAR(64),
  tipe_text VARCHAR(32),
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cctv_list_quarantine AS FROM cctv_list LIMIT 0;
CREATE TABLE IF NOT EXISTS cctv_list_final AS FROM cctv_list LIMIT 0
-- Foreign keys
--ALTER TABLE cctv_list ADD FOREIGN KEY (adm2) REFERENCES wilayah_provinsi_kabupaten (kode);
--ALTER TABLE vehicle_speed ADD FOREIGN KEY (camera_code) REFERENCES cctv_list (camera_code);
--ALTER TABLE hourly_vehicle_speed ADD FOREIGN KEY (camera_code) REFERENCES cctv_list (camera_code);
--ALTER TABLE all_timeseries_vehicle_speed ADD FOREIGN KEY (camera_code) REFERENCES cctv_list (camera_code);
--ALTER TABLE bmkg_weather ADD FOREIGN KEY (adm1) REFERENCES wilayah_provinsi_kabupaten (kode);
--ALTER TABLE bmkg_weather ADD FOREIGN KEY (adm2) REFERENCES wilayah_provinsi_kabupaten (kode);
--ALTER TABLE bmkg_weather ADD FOREIGN KEY (adm3) REFERENCES wilayah_kecamatan_kelurahan (kode);
--ALTER TABLE bmkg_weather ADD FOREIGN KEY (adm4) REFERENCES wilayah_kecamatan_kelurahan (kode);
