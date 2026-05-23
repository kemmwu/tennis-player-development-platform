with source as (
    select * from {{ source('bronze', 'raw_students') }}
),

cleaned as (
    select
        student_id,
        intake_id,
        full_name,
        chinese_name,
        trim(lower(preferred_name))                     as preferred_name,
        date_of_birth,
        try_cast(utr_rating as double)                  as utr_rating,
        lower(dominant_hand)                            as dominant_hand,
        height,
        try_cast(years_playing as int)                  as years_playing,
        try_cast(training_frequency_per_week as int)    as training_frequency_per_week,
        lower(competition_level)                        as competition_level,
        goals,
        injury_history,
        previous_coaching,
        contact_name,
        contact_email,
        submitted_at,
        _ingested_at,
        is_current,
        valid_from,
        valid_to
    from source
    where is_current = true
)

select * from cleaned
