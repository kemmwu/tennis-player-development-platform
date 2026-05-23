{{
    config(
        materialized='view'
    )
}}

with matches as (
    select * from {{ ref('int_match_with_opponent_strength') }}
),

training as (
    select * from {{ ref('int_session_rollup_daily') }}
),

/*
  Links coaching notes to subsequent match performance using a 7-day window.

  DOMAIN RATIONALE (documented in /docs/decisions.md):
  A coaching note written after a training session typically targets the
  next match within 7 days. Notes written more than 7 days before a match
  are unlikely to be directly related to that match's outcome. This 7-day
  window is based on real coaching practice — most students compete weekly.

  JOIN LOGIC:
  - For each training session with a note, find matches played by the same
    player within the next 7 days
  - If multiple matches fall within the window, take the closest one
  - If no match falls within the window, the note is unlinked (null match fields)
*/

training_with_notes as (
    select *
    from training
    where true  -- all daily rollups; note linkage happens via match join
),

note_to_match as (
    select
        t.player_id,
        t.session_date                                      as training_date,
        t.avg_shots_in,
        t.avg_forehand_cross_court_in,
        t.avg_backhand_cross_court_in,
        t.sessions_on_day,

        -- Linked match fields (null if no match within 7 days)
        m.match_id,
        m.match_date,
        m.player_won,
        m.weighted_win,
        m.opponent_strength_weight,
        m.total_points_won_pct,
        m.break_point_conversion,
        m.first_serve_pct,
        m.raw_note_text,

        -- Days between training and linked match
        case
            when m.match_date is not null
            then datediff(m.match_date, t.session_date)
        end                                                 as days_to_match,

        -- Flag: was a match played within 7 days of this training?
        case
            when m.match_date is not null then true
            else false
        end                                                 as has_linked_match

    from training_with_notes t
    left join matches m
        on  t.player_id   = m.player_id
        and m.match_date  between t.session_date and date_add(t.session_date, 7)
),

/*
  Deduplicate: if multiple matches fall within the 7-day window,
  keep only the closest match (smallest days_to_match).
*/

ranked as (
    select
        *,
        row_number() over (
            partition by player_id, training_date
            order by days_to_match asc nulls last
        )                                                   as match_rank
    from note_to_match
),

final as (
    select * from ranked where match_rank = 1
)

select * from final
