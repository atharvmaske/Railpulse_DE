{{
  config(
    materialized = 'table'
  )
}}

with route_perf as (
    select * from {{ ref('mart_route_performance') }}
),

network_summary as (
    select
        event_date,

        count(distinct route_id) as active_routes,
        sum(total_events) as total_trip_events,
        sum(on_time_count) as total_on_time,
        sum(severe_delay_count) as total_severe_delays,

        round(avg(on_time_rate_pct)::numeric, 2) as network_on_time_rate_pct,
        round(avg(avg_delay_minutes)::numeric, 2) as network_avg_delay_minutes,
        round(max(max_delay_seconds) / 60.0, 1) as worst_delay_minutes,

        count(*) filter (where performance_grade = 'A') as grade_a_routes,
        count(*) filter (where performance_grade = 'F') as grade_f_routes,
        count(*) filter (where slo_breached = true) as slo_breached_routes,

        current_timestamp as dbt_updated_at

    from route_perf
    group by event_date
)

select * from network_summary