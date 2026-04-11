# Table: air_quality

| Column               | Type      | Description |
|----------------------|-----------|-------------|
| id_stasiun           | varchar   | Station ID |
| waktu                | timestamp | Timestamp |
| lat                  | float     | Latitude |
| lon                  | float     | Longitude |
| a_pm10               | float     | Actual PM10 |
| a_pm25               | float     | Actual PM2.5 |
| a_so2                | float     | Actual SO2 |
| a_co                 | float     | Actual CO |
| a_o3                 | float     | Actual O3 |
| a_no2                | float     | Actual NO2 |
| a_hc                 | float     | Actual HC |
| c_pm10               | float     | Calculated PM10 |
| c_pm25               | float     | Calculated PM2.5 |
| c_so2                | float     | Calculated SO2 |
| c_co                 | float     | Calculated CO |
| c_o3                 | float     | Calculated O3 |
| c_no2                | float     | Calculated NO2 |
| c_hc                 | float     | Calculated HC |
| t_pm10               | float     | Threshold PM10 |
| t_pm25               | float     | Threshold PM2.5 |
| t_so2                | float     | Threshold SO2 |
| t_co                 | float     | Threshold CO |
| t_o3                 | float     | Threshold O3 |
| t_no2                | float     | Threshold NO2 |
| t_hc                 | float     | Threshold HC |
| nama                 | varchar   | Station name |
| alamat               | varchar   | Address |
| kota                 | varchar   | City |
| provinsi             | varchar   | Province |
| param                | varchar   | Main parameter |
| stasiun_uji          | varchar   | Testing station flag/info |
| stasiun_show         | varchar   | Display flag |
| stasiun_show_detail  | varchar   | Detailed display flag |
| p3e                  | varchar   | P3E info |
| time_offset          | integer   | Time offset |
| time_z               | varchar   | Timezone |
| is_maintenance       | boolean   | Maintenance status |
| auto_validation      | boolean   | Auto validation flag |
| tipe                 | varchar   | Station type |
| kelurahan            | varchar   | Subdistrict |
| kecamatan            | varchar   | District |
| val                  | float     | Air quality value |
| cat                  | varchar   | Category code |
| kategori             | json      | Category detail (JSON) |
| waktu_text           | varchar   | Human-readable time |
| tipe_text            | varchar   | Type description |