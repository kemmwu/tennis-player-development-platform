with source as (
    select * from {{ source('bronze', 'raw_match_extractions') }}
),

name_map as (
    select * from {{ ref('stg_player_name_mapping') }}
),

cleaned as (
    select
        s.match_id,
        coalesce(nm.canonical_player_id, lower(s.player_id))   as player_id,
        cast(s.match_date as date)                              as match_date,
        s.session_time,
        s.opponent_name,
        s.opponent_utr,
        s.tournament_name,
        lower(s.surface)                                        as surface,
        s.score,
        s.player_won,

        -- Point stats
        s.winners,
        s.unforced_errors,
        s.forehand_winners,
        s.forehand_unforced_errors,
        s.backhand_winners,
        s.backhand_unforced_errors,
        s.total_points_won,
        s.total_points_played,

        -- Calculated point pct
        case
            when s.total_points_played > 0
            then round(s.total_points_won / s.total_points_played, 4)
        end                                                     as total_points_won_pct,

        -- Break points
        s.break_points_won,
        s.break_points_total,
        case
            when s.break_points_total > 0
            then round(s.break_points_won / s.break_points_total, 4)
        end                                                     as break_point_conversion,

        s.break_points_saved,
        s.break_points_saved_total,
        case
            when s.break_points_saved_total > 0
            then round(s.break_points_saved / s.break_points_saved_total, 4)
        end                                                     as break_points_saved_pct,

        -- Serve stats
        s.aces,
        s.service_winners,
        s.double_faults,
        s.first_serves_in,
        s.first_serves_total,
        case
            when s.first_serves_total > 0
            then round(s.first_serves_in / s.first_serves_total, 4)
        end                                                     as first_serve_pct,

        s.second_serves_in,
        s.second_serves_total,
        case
            when s.second_serves_total > 0
            then round(s.second_serves_in / s.second_serves_total, 4)
        end                                                     as second_serve_pct,

        s.first_serves_won,
        s.first_serves_won_total,
        case
            when s.first_serves_won_total > 0
            then round(s.first_serves_won / s.first_serves_won_total, 4)
        end                                                     as first_serves_won_pct,

        s.second_serves_won,
        s.second_serves_won_total,
        case
            when s.second_serves_won_total > 0
            then round(s.second_serves_won / s.second_serves_won_total, 4)
        end                                                     as second_serves_won_pct,

        -- Return stats
        s.return_points_won,
        s.return_points_total,
        case
            when s.return_points_total > 0
            then round(s.return_points_won / s.return_points_total, 4)
        end                                                     as return_points_won_pct,

        s.first_returns_won,
        s.first_returns_total,
        case
            when s.first_returns_total > 0
            then round(s.first_returns_won / s.first_returns_total, 4)
        end                                                     as first_returns_won_pct,

        s.second_returns_won,
        s.second_returns_total,
        case
            when s.second_returns_total > 0
            then round(s.second_returns_won / s.second_returns_total, 4)
        end                                                     as second_returns_won_pct,

        -- Rally stats
        s.rallies_1_4_won,
        s.rallies_1_4_total,
        case
            when s.rallies_1_4_total > 0
            then round(s.rallies_1_4_won / s.rallies_1_4_total, 4)
        end                                                     as rallies_1_4_won_pct,

        s.rallies_5_8_won,
        s.rallies_5_8_total,
        case
            when s.rallies_5_8_total > 0
            then round(s.rallies_5_8_won / s.rallies_5_8_total, 4)
        end                                                     as rallies_5_8_won_pct,

        s.rallies_9plus_won,
        s.rallies_9plus_total,
        case
            when s.rallies_9plus_total > 0
            then round(s.rallies_9plus_won / s.rallies_9plus_total, 4)
        end                                                     as rallies_9plus_won_pct,

        -- Notes
        s.raw_note_text,
        s.extraction_confidence,
        s.prompt_version,
        s._ingested_at

    from source s
    left join name_map nm
        on lower(s.player_id) = lower(nm.alias)
),

validated as (
    select *
    from cleaned
    where match_date is not null
      and player_id  is not null
      and winners    >= 0
      and unforced_errors >= 0
)

select * from validated
