# Table: hourly_vehicle_speed

| Column              | Type    | Description |
|---------------------|--------|-------------|
| camera_code         | varchar | Camera identifier |
| hourly_label        | json    | Hourly time labels (e.g. 00:00–23:00) |
| listdata60_60_sm    | json    | Speed data for small vehicles (SM) |
| listdata60_60_mp    | json    | Speed data for medium passenger (MP) |
| listdata60_60_ks    | json    | Speed data for large vehicles (KS) |
| listdata60_60_bb    | json    | Speed data for heavy vehicles (BB) |
| listdata60_60_tb    | json    | Speed data for trucks/buses (TB) |
| totalvolume_sm      | integer | Total volume small vehicles |
| totalvolume_mp      | integer | Total volume medium passenger |
| totalvolume_ks      | integer | Total volume large vehicles |
| totalvolume_bb      | integer | Total volume heavy vehicles |
| totalvolume_tb      | integer | Total volume trucks/buses |


# Table: vehicle_speed

| Column                         | Type     | Description |
|--------------------------------|----------|-------------|
| camera_code                    | varchar  | Camera identifier |
| last_update_5minutes           | timestamp| Last update timestamp (5-minute interval) |
| label5min_data                 | json     | Labels for 5-minute intervals |
| listchart1_normal              | json     | Chart data for normal direction |
| listchart1_opposite            | json     | Chart data for opposite direction |
| speed_normal                   | json     | Average speed (normal direction) |
| speed_opposite                 | json     | Average speed (opposite direction) |
| speed_gol_normal               | json     | Speed by vehicle group (normal) |
| speed_gol_opposite             | json     | Speed by vehicle group (opposite) |
| speed_max_gol_normal_list      | json     | Max speed per vehicle group (normal) |
| speed_max_gol_opposite_list    | json     | Max speed per vehicle group (opposite) |