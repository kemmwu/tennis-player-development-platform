{{
    config(
        materialized='table'
    )
}}

/*
  Player Development Score — Flagship Metric
  ============================================
  Composite score 0-100 combining four components:

  Component 1: Win Rate Trend (40% weight)
    Rolling 30-day win rate weighted by opponent strength.
    A player winning against stronger opponents scores higher.

  Component 2: Opponent Strength Score (30% weight)
    Average opponent strength weight across recent matches.
    Reflects the quality of competition the player is facing.

  Component 3: Technique Progression (20% weight)
    Based on training session accuracy trends.
    Uses forehand + backhand cross-court accuracy as proxy.
    Null when no training data available in the window.

  Component 4: Break Point Conversion (10% weight)
    Average break point conversion rate over recent matches.
    The most high-leverage match statistic in tennis.

  Formula documented here and in /docs/decisions.md.
*/

with matches as (
    select * from {{ ref('fct_match_performance') }}
),

training as (
    select * from {{ ref('fct_training_sessions') }}
),

-- Rolling 30-day match window per player
match_window as (
    select
        player_id,
        player_sk,
        date_trunc('week', match_date)                  as score_week,

        -- Win rate trend (weighted by opponent strength)
        round(
            sum(weighted_win) / nullif(count(*), 0)
        , 4)                                            as weighted_win_rate,

        -- Opponent strength score
        round(avg(opponent_strength_weight), 4)         as avg_opponent_strength,

        -- Break point conversion
        round(avg(break_point_conversion), 4)           as avg_break_point_conversion,

        -- Match volume
        count(*)                                        as matches_played,
        sum(case when player_won then 1 else 0 end)     as matches_won

    from matches
    group by
        player_id,
        player_sk,
        date_trunc('week', match_date)
),

-- Rolling 30-day training window per player
training_window as (
    select
        player_id,
        player_sk,
        date_trunc('week', session_date)                as score_week,

        -- Technique progression proxy
        round(avg(
            (coalesce(avg_forehand_cross_court_in, 0)
            + coalesce(avg_backhand_cross_court_in, 0)) / 2.0
        ), 4)                                           as avg_technique_score,

        count(*)                                        as sessions_completed,
        round(avg(total_shots_per_hour), 1)             as avg_shots_per_hour

    from training
    group by
        player_id,
        player_sk,
        date_trunc('week', session_date)
),

-- Combine match and training windows
combined as (
    select
        coalesce(m.player_id, t.player_id)              as player_id,
        coalesce(m.player_sk, t.player_sk)              as player_sk,
        coalesce(m.score_week, t.score_week)            as score_week,

        -- Match components
        coalesce(m.weighted_win_rate, 0)                as weighted_win_rate,
        coalesce(m.avg_opponent_strength, 1.0)          as avg_opponent_strength,
        coalesce(m.avg_break_point_conversion, 0)       as avg_break_point_conversion,
        coalesce(m.matches_played, 0)                   as matches_played,
        coalesce(m.matches_won, 0)                      as matches_won,

        -- Training components
        coalesce(t.avg_technique_score, 0)              as avg_technique_score,
        coalesce(t.sessions_completed, 0)               as sessions_completed,
        coalesce(t.avg_shots_per_hour, 0)               as avg_shots_per_hour

    from match_window m
    full outer join training_window t
        on  m.player_id  = t.player_id
        and m.score_week = t.score_week
),

/*
  Development Score Calculation:
  Each component is first normalized to 0-100 range,
  then weighted and summed.

  Win rate:           0.0-2.0 weighted win rate → * 50 → 0-100
  Opponent strength:  0.5-2.0 → normalize to 0-100
  Technique:          0.0-1.0 accuracy → * 100 → 0-100
  Break point:        0.0-1.0 → * 100 → 0-100
*/

scored as (
    select
        player_id,
        player_sk,
        score_week,

        -- Raw components
        weighted_win_rate,
        avg_opponent_strength,
        avg_technique_score,
        avg_break_point_conversion,
        matches_played,
        matches_won,
        sessions_completed,
        avg_shots_per_hour,

        -- Normalized components (0-100)
        round(least(100, weighted_win_rate * 50), 2)    as win_rate_score,
        round(least(100, greatest(0,
            (avg_opponent_strength - 0.5) / 1.5 * 100
        )), 2)                                          as opponent_strength_score,
        round(least(100, avg_technique_score * 100), 2) as technique_score,
        round(least(100,
            avg_break_point_conversion * 100
        ), 2)                                           as break_point_score,

        -- Composite development score (weighted sum)
        round(
            (least(100, weighted_win_rate * 50)         * 0.40)
          + (least(100, greatest(0,
                (avg_opponent_strength - 0.5) / 1.5 * 100
            ))                                          * 0.30)
          + (least(100, avg_technique_score * 100)      * 0.20)
          + (least(100,
                avg_break_point_conversion * 100
            )                                           * 0.10)
        , 2)                                            as development_score,

        current_timestamp()                             as calculated_at

    from combined
)

select * from scored
