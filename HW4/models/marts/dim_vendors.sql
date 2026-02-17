with trips_unioned as (
    select * FROM {{ref('int_trips_union')}}
),

vendors as (
    SELECT 
        distinct vendor_id,
        {{ get_vendor_names('vendor_id') }} as vendor_name
    from trips_unioned
)

select * from vendors