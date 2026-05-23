with source as (
    select * from {{ source('bronze', 'raw_training_sessions') }}
),

name_map as (
    select * from {{ ref('stg_player_name_mapping') }}
),

cleaned as (
    select
        s.session_id,
        coalesce(nm.canonical_player_id, lower(s.player_id))   as player_id,
        cast(s.session_date as date)                            as session_date,
        s.session_time,
        s.session_type,
        s.drills_completed,
        s.raw_note_text,

        -- Overall stats — stored as decimals (0-1) in Bronze
        s.shots_in,
        s.shots_per_hour,
        s.longest_rally,
        s.rallies_above_5_shots,

        -- Serve stats (nullable — only present if serves were hit)
        s.serves_in_ad,
        s.serves_in_deuce,
        s.avg_serve_speed_ad,
        s.avg_serve_speed_deuce,

        -- Return stats (nullable)
        s.returns_in_ad,
        s.returns_in_deuce,
        s.avg_return_speed_ad,
        s.avg_return_speed_deuce,

        -- Forehand stats
        s.forehand_cross_court_in,
        s.forehand_down_the_line_in,
        s.forehand_avg_cross_court_speed,
        s.forehand_avg_down_the_line_speed,
        s.forehand_cross_court_deep,
        s.forehand_down_the_line_deep,

        -- Backhand stats
        s.backhand_cross_court_in,
        s.backhand_down_the_line_in,
        s.backhand_avg_cross_court_speed,
        s.backhand_avg_down_the_line_speed,
        s.backhand_cross_court_deep,
        s.backhand_down_the_line_deep,

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
    where session_date is not null
      and player_id    is not null
      and shots_in between 0 and 1
)

select * from validated
