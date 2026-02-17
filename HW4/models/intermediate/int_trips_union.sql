with green_tripdata as(
        select * from {{ ref('stg_green_tripdata') }}
),

yellow_tripdata as(
    select * from {{ ref('stg_yellow_tripdata') }}
),

trips_unioned as (
    select * FROM green_tripdata
    union all
    select * FROM yellow_tripdata
)

SELECT * from trips_unioned