{{
  config(
    materialized = 'view'
  )
}}

with positions as (
    select * from {{ ref('stg_vehicle_positions') }}
    where is_in_germany = true
),

vehicle_summary as (
    select
        vehicle_id,
        route_id,
        date_trunc('hour', recorded_at) as hour_bucket,

        count(*) as position_count,
        round(avg(speed)::numeric, 1) as avg_speed_kmh,
        max(speed) as max_speed_kmh,
        count(*) filter (where speed_category = 'stopped') as stopped_count,

        (array_agg(latitude order by recorded_at desc))[1] as last_latitude,
        (array_agg(longitude order by recorded_at desc))[1] as last_longitude

    from positions
    group by vehicle_id, route_id, date_trunc('hour', recorded_at)
)

select * from vehicle_summary