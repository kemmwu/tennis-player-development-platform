{{
    config(
        materialized='view'
    )
}}

with matches as (
    select * from {{ ref('stg_match_stats') }}
),

with_strength as (
    select
        match_id,
        player_id,
        match_date,
        session_time,
        score,
        player_won,

        -- Opponent strength defaults to 1.0 (neutral)
        -- until opponent UTR data is available
        1.0                                             as opponent_strength_weight,

        case
            when player_won = true then 1.0
            else 0.0
        end                                             as weighted_win,

        -- Core stats
        winners,
        unforced_errors,
        forehand_winners,
        backhand_winners,
        total_points_won_pct,
        break_point_conversion,
        break_points_saved_pct,
        first_serve_pct,
        second_serve_pct,
        first_serves_won_pct,
        second_serves_won_pct,
        return_points_won_pct,
        rallies_1_4_won_pct,
        rallies_5_8_won_pct,
        rallies_9plus_won_pct,
        raw_note_text,
        extraction_confidence,

        -- Rolling window helpers
        date_trunc('week', match_date)                  as match_week,
        date_trunc('month', match_date)                 as match_month,
        date_format(match_date, 'yyyy-MM')              as match_year_month

    from matches
)

select * from with_strength
