{{
  config(
    materialized = 'view'
  )
}}

with source as (
    select * from {{ source('railpulse_raw', 'vehicle_positions') }}
),

cleaned as (
    select
        id as position_event_id,
        vehicle_id,
        trip_id,
        route_id,
        latitude,
        longitude,
        speed,

        case
            when speed is null then 'unknown'
            when speed = 0 then 'stopped'
            when speed < 50 then 'slow'
            when speed between 50 and 160 then 'normal'
            else 'fast'
        end as speed_category,

        recorded_at,
        created_at,

        case
            when latitude between 47.0 and 55.5
             and longitude between 5.5 and 15.5
            then true
            else false
        end as is_in_germany

    from source
    where vehicle_id is not null
)

select * from cleaned