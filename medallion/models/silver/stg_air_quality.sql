{{ config(materialized='table') }}

{% if target.type == 'bigquery' %}

WITH source AS (
    SELECT *
    FROM {{ source('sources', 'air_quality') }}
),

parsed AS (
    SELECT
        *,
        PARSE_JSON(SAFE_CAST(kategori AS STRING)) AS kategori_json
    FROM source
)

SELECT
    CAST(id_stasiun AS STRING) AS station_id,
    SAFE_CAST(waktu AS TIMESTAMP) AS observed_at,
    EXTRACT(DAY FROM SAFE_CAST(waktu AS TIMESTAMP)) AS day,
    EXTRACT(MONTH FROM SAFE_CAST(waktu AS TIMESTAMP)) AS month,
    EXTRACT(YEAR FROM SAFE_CAST(waktu AS TIMESTAMP)) AS year,

    CAST(nama AS STRING) AS station_name,
    CAST(alamat AS STRING) AS address,
    CAST(kelurahan AS STRING) AS kelurahan,
    CAST(kecamatan AS STRING) AS kecamatan,
    CAST(kota AS STRING) AS city,
    CAST(provinsi AS STRING) AS province,
    CAST(p3e AS STRING) AS p3e,
    SAFE_CAST(lat AS FLOAT64) AS latitude,
    SAFE_CAST(lon AS FLOAT64) AS longitude,

    SAFE_CAST(a_pm10 AS FLOAT64) AS pm10_value,
    SAFE_CAST(a_pm25 AS FLOAT64) AS pm25_value,
    SAFE_CAST(a_so2 AS FLOAT64) AS so2_value,
    SAFE_CAST(a_co AS FLOAT64) AS co_value,
    SAFE_CAST(a_o3 AS FLOAT64) AS o3_value,
    SAFE_CAST(a_no2 AS FLOAT64) AS no2_value,
    SAFE_CAST(a_hc AS FLOAT64) AS hc_value,

    SAFE_CAST(c_pm10 AS INT64) AS pm10_index,
    SAFE_CAST(c_pm25 AS INT64) AS pm25_index,
    SAFE_CAST(c_so2 AS INT64) AS so2_index,
    SAFE_CAST(c_co AS INT64) AS co_index,
    SAFE_CAST(c_o3 AS INT64) AS o3_index,
    SAFE_CAST(c_no2 AS INT64) AS no2_index,
    SAFE_CAST(c_hc AS INT64) AS hc_index,

    SAFE_CAST(t_pm10 AS INT64) AS pm10_time,
    SAFE_CAST(t_pm25 AS INT64) AS pm25_time,
    SAFE_CAST(t_so2 AS INT64) AS so2_time,
    SAFE_CAST(t_co AS INT64) AS co_time,
    SAFE_CAST(t_o3 AS INT64) AS o3_time,
    SAFE_CAST(t_no2 AS INT64) AS no2_time,
    SAFE_CAST(t_hc AS INT64) AS hc_time,

    CAST(param AS STRING) AS dominant_param,
    CASE
        WHEN param IS NULL OR TRIM(CAST(param AS STRING)) = '' THEN NULL
        ELSE TO_JSON_STRING(
            ARRAY(
                SELECT TRIM(part)
                FROM UNNEST(
                    SPLIT(REGEXP_REPLACE(CAST(param AS STRING), r'<sub>([^<]*)</sub>', r'\1'), ',')
                ) AS part
                WHERE TRIM(part) <> ''
            )
        )
    END AS dominant_params_json,

    SAFE_CAST(val AS INT64) AS aqi_value,
    CAST(cat AS STRING) AS aqi_category,

    SAFE_CAST(JSON_VALUE(kategori_json, '$.nilai_uid') AS INT64) AS kategori_id,
    CAST(JSON_VALUE(kategori_json, '$.nilai') AS STRING) AS kategori_nilai,
    CAST(JSON_VALUE(kategori_json, '$.keterangan') AS STRING) AS kategori_keterangan,

    CAST(tipe AS STRING) AS station_type_code,
    CAST(tipe_text AS STRING) AS station_type,
    CAST(stasiun_uji AS STRING) AS stasiun_uji,
    CAST(stasiun_show AS STRING) AS stasiun_show,
    CAST(stasiun_show_detail AS STRING) AS stasiun_show_detail,
    CAST(is_maintenance AS STRING) AS is_maintenance,
    CAST(auto_validation AS STRING) AS auto_validation,
    CAST(time_offset AS STRING) AS time_offset,
    CAST(time_z AS STRING) AS time_zone,
    CAST(waktu_text AS STRING) AS observed_at_text
FROM parsed

{% else %}

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

{% endif %}
