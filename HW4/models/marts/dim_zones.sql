with taxi_zone_lookup as (
    select * FROM {{ref('taxi_zone_lookup')}}
),

renamed as (
    SELECT  locationid as location_id,
    borough,
    zone,
    service_zone
    from taxi_zone_lookup
)

SELECT * FROM taxi_zone_lookup