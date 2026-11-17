{{
  config(
    materialized = 'view'
  )
}}

with source as (
    select * from {{ source('railpulse_raw', 'trip_delays') }}
),

cleaned as (
    select
        id as trip_event_id,
        trip_id,
        route_id,
        stop_id,
        delay_seconds,

        case
            when delay_seconds is null then 'unknown'
            when delay_seconds <= 0 then 'on_time'
            when delay_seconds between 1 and 60 then 'minor'
            when delay_seconds between 61 and 300 then 'moderate'
            else 'severe'
        end as delay_category,

        round(delay_seconds / 60.0, 1) as delay_minutes,

        event_date,
        ingested_at,

        case
            when trip_id is null then false
            when route_id is null then false
            else true
        end as is_valid

    from source
    where trip_id is not null
)

select * from cleaned