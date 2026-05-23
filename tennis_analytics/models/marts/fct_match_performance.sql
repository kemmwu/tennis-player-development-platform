

with matches as (
    select * from {{ ref('int_match_with_opponent_strength') }}
),

players as (
    select player_id, player_sk
    from {{ ref('dim_players') }}
    where is_current = true
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['match_id']) }}
                                                        as match_sk,
        m.match_id,
        p.player_sk,
        m.player_id,
        m.match_date,
        m.session_time,
        m.match_year_month,
        m.match_week,
        m.match_month,
        m.score,
        m.player_won,

        -- Opponent strength
        m.opponent_strength_weight,
        m.weighted_win,

        -- Point stats
        m.winners,
        m.unforced_errors,
        m.forehand_winners,
        m.backhand_winners,
        m.total_points_won_pct,

        -- Break points
        m.break_point_conversion,
        m.break_points_saved_pct,

        -- Serve stats
        m.first_serve_pct,
        m.second_serve_pct,
        m.first_serves_won_pct,
        m.second_serves_won_pct,

        -- Return stats
        m.return_points_won_pct,

        -- Rally stats
        m.rallies_1_4_won_pct,
        m.rallies_5_8_won_pct,
        m.rallies_9plus_won_pct,

        -- Notes
        case when m.raw_note_text is not null
             then true else false end                   as has_note,
        m.extraction_confidence

    from matches m
    left join players p
        on m.player_id = p.player_id
)

select * from final
