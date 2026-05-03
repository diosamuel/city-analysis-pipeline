{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('sources', 'air_quality') }}
)

SELECT
    id_stasiun::VARCHAR                           AS station_id,
    waktu::TIMESTAMP                              AS observed_at,
    EXTRACT(DAY   FROM waktu::TIMESTAMP)          AS day,
    EXTRACT(MONTH FROM waktu::TIMESTAMP)          AS month,
    EXTRACT(YEAR  FROM waktu::TIMESTAMP)          AS year,

    nama::VARCHAR                                 AS station_name,
    alamat::VARCHAR                               AS address,
    kelurahan::VARCHAR                            AS kelurahan,
    kecamatan::VARCHAR                            AS kecamatan,
    kota::VARCHAR                                 AS city,
    provinsi::VARCHAR                             AS province,
    p3e::VARCHAR                                  AS p3e,
    lat::DOUBLE                                   AS latitude,
    lon::DOUBLE                                   AS longitude,

    a_pm10::DOUBLE                                AS pm10_value,
    a_pm25::DOUBLE                                AS pm25_value,
    a_so2::DOUBLE                                 AS so2_value,
    a_co::DOUBLE                                  AS co_value,
    a_o3::DOUBLE                                  AS o3_value,
    a_no2::DOUBLE                                 AS no2_value,
    a_hc::DOUBLE                                  AS hc_value,

    c_pm10::SMALLINT                              AS pm10_index,
    c_pm25::SMALLINT                              AS pm25_index,
    c_so2::SMALLINT                               AS so2_index,
    c_co::SMALLINT                                AS co_index,
    c_o3::SMALLINT                                AS o3_index,
    c_no2::SMALLINT                               AS no2_index,
    c_hc::SMALLINT                                AS hc_index,

    t_pm10::INTEGER                               AS pm10_time,
    t_pm25::INTEGER                               AS pm25_time,
    t_so2::INTEGER                                AS so2_time,
    t_co::INTEGER                                 AS co_time,
    t_o3::INTEGER                                 AS o3_time,
    t_no2::INTEGER                                AS no2_time,
    t_hc::INTEGER                                 AS hc_time,

    param::VARCHAR                                AS dominant_param,
    CASE
        WHEN param IS NULL OR trim(param) = '' THEN NULL
        ELSE to_json(
            list_transform(
                string_split(
                    regexp_replace(param, '<sub>([^<]*)</sub>', '\1', 'g'),
                    ','
                ),
                x -> trim(x)
            )
        )
    END                                           AS dominant_params_json,

    val::INTEGER                                  AS aqi_value,
    cat::VARCHAR                                  AS aqi_category,

    (kategori ->> 'nilai_uid')::INTEGER           AS kategori_id,
    (kategori ->> 'nilai')::VARCHAR               AS kategori_nilai,
    (kategori ->> 'keterangan')::VARCHAR          AS kategori_keterangan,

    tipe::VARCHAR                                 AS station_type_code,
    tipe_text::VARCHAR                            AS station_type,
    stasiun_uji::VARCHAR                          AS stasiun_uji,
    stasiun_show::VARCHAR                         AS stasiun_show,
    stasiun_show_detail::VARCHAR                  AS stasiun_show_detail,
    is_maintenance::VARCHAR                       AS is_maintenance,
    auto_validation::VARCHAR                      AS auto_validation,
    time_offset::VARCHAR                          AS time_offset,
    time_z::VARCHAR                               AS time_zone,
    waktu_text::VARCHAR                           AS observed_at_text
FROM source


-- normalization the time difference between time_offset
-- SELECT observed_at, saq.observed_at_text, saq.time_zone FROM gold.main.stg_air_quality AS saq