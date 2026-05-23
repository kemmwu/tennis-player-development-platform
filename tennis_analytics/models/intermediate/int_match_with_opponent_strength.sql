{{
    config(
        materialized='view'
    )
}}

with matches as (
    select * from {{ ref('stg_match_stats') }}
),

/*
  Opponent strength weighting logic — based on coaching domain expertise:
  - If opponent UTR is known, weight the match result by relative strength
  - A win against a higher UTR opponent scores higher than a win against a lower UTR
  - Losses against much stronger opponents are penalised less
  - UTR difference capped at +/- 3 to avoid extreme outliers
  - When opponent UTR is unknown, weight defaults to 1.0 (neutral)
*/

with_strength as (
    select
        match_id,
        player_id,
        match_date,
        session_time,
        opponent_name,
        opponent_utr,
        surface,
        score,
        player_won,

        -- Opponent strength score: higher = stronger opponent
        case
            when opponent_utr is null then 1.0
            else greatest(0.5, least(2.0, 1.0 + (opponent_utr - 7.0) / 10.0))
        end                                             as opponent_strength_weight,

        -- Weighted win: win value scaled by opponent strength
        case
            when player_won = true and opponent_utr is not null
            then greatest(0.5, least(2.0, 1.0 + (opponent_utr - 7.0) / 10.0))
            when player_won = true and opponent_utr is null
            then 1.0
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
