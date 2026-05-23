{{
    config(
        materialized='view'
    )
}}

with training as (
    select * from {{ ref('stg_training_sessions') }}
),

/*
  Daily rollup of training sessions per player.
  A player may have multiple training sessions on the same day
  (e.g. morning and evening sessions with different time identifiers).
  This model aggregates them into a single daily summary per player.
*/

daily_rollup as (
    select
        player_id,
        session_date,

        -- Session counts
        count(*)                                            as sessions_on_day,
        sum(shots_per_hour)                                 as total_shots_per_hour,

        -- Accuracy averages across sessions
        round(avg(shots_in), 4)                             as avg_shots_in,
        round(avg(rallies_above_5_shots), 4)                as avg_rallies_above_5_shots,
        max(longest_rally)                                  as max_longest_rally,

        -- Serve averages (nullable — only where serves were hit)
        round(avg(serves_in_ad), 4)                         as avg_serves_in_ad,
        round(avg(serves_in_deuce), 4)                      as avg_serves_in_deuce,
        round(avg(avg_serve_speed_ad), 1)                   as avg_serve_speed_ad,
        round(avg(avg_serve_speed_deuce), 1)                as avg_serve_speed_deuce,

        -- Return averages
        round(avg(returns_in_ad), 4)                        as avg_returns_in_ad,
        round(avg(returns_in_deuce), 4)                     as avg_returns_in_deuce,

        -- Forehand averages
        round(avg(forehand_cross_court_in), 4)              as avg_forehand_cross_court_in,
        round(avg(forehand_down_the_line_in), 4)            as avg_forehand_down_the_line_in,
        round(avg(forehand_avg_cross_court_speed), 1)       as avg_forehand_cross_court_speed,
        round(avg(forehand_avg_down_the_line_speed), 1)     as avg_forehand_down_the_line_speed,
        round(avg(forehand_cross_court_deep), 4)            as avg_forehand_cross_court_deep,
        round(avg(forehand_down_the_line_deep), 4)          as avg_forehand_down_the_line_deep,

        -- Backhand averages
        round(avg(backhand_cross_court_in), 4)              as avg_backhand_cross_court_in,
        round(avg(backhand_down_the_line_in), 4)            as avg_backhand_down_the_line_in,
        round(avg(backhand_avg_cross_court_speed), 1)       as avg_backhand_cross_court_speed,
        round(avg(backhand_avg_down_the_line_speed), 1)     as avg_backhand_down_the_line_speed,
        round(avg(backhand_cross_court_deep), 4)            as avg_backhand_cross_court_deep,
        round(avg(backhand_down_the_line_deep), 4)          as avg_backhand_down_the_line_deep,

        -- Extraction quality
        round(avg(extraction_confidence), 4)                as avg_extraction_confidence,

        -- Time helpers
        date_trunc('week', session_date)                    as session_week,
        date_trunc('month', session_date)                   as session_month,
        date_format(session_date, 'yyyy-MM')                as session_year_month

    from training
    group by
        player_id,
        session_date
)

select * from daily_rollup
