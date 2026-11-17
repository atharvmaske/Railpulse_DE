{{
  config(
    materialized = 'view'
  )
}}

with trip_updates as (
    select * from {{ ref('stg_trip_updates') }}
    where is_valid = true
),

route_aggregates as (
    select
        route_id,
        event_date,

        count(*) as total_events,
        count(*) filter (where delay_category = 'on_time') as on_time_count,
        count(*) filter (where delay_category = 'minor') as minor_delay_count,
        count(*) filter (where delay_category = 'moderate') as moderate_delay_count,
        count(*) filter (where delay_category = 'severe') as severe_delay_count,

        round(avg(delay_seconds)::numeric, 1) as avg_delay_seconds,
        round(avg(delay_minutes)::numeric, 2) as avg_delay_minutes,
        max(delay_seconds) as max_delay_seconds,
        percentile_cont(0.5) within group (order by delay_seconds) as median_delay_seconds,
        percentile_cont(0.95) within group (order by delay_seconds) as p95_delay_seconds,

        round(
            100.0 * count(*) filter (where delay_category = 'on_time')
            / nullif(count(*), 0),
            2
        ) as on_time_rate_pct

    from trip_updates
    group by route_id, event_date
)

select * from route_aggregates