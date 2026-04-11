# Table: bmkg_weather

| Column        | Type    | Description |
|--------------|--------|-------------|
| adm1         | varchar | Administrative level 1 |
| adm2         | varchar | Administrative level 2 |
| adm3         | varchar | Administrative level 3 |
| adm4         | varchar | Administrative level 4 |
| provinsi     | varchar | Province name |
| kotkab       | varchar | City / Regency |
| kecamatan    | varchar | District |
| desa         | varchar | Village |
| lon          | float   | Longitude |
| lat          | float   | Latitude |
| timezone     | varchar | Timezone |
| weather_data | json    | Weather data (BMKG response) |