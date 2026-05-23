

with students as (
    select * from {{ ref('stg_students') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['student_id', 'valid_from']) }}
                                                        as player_sk,
        student_id                                      as player_id,
        preferred_name,
        full_name,
        chinese_name,
        date_of_birth,
        utr_rating,
        age_group,
        dominant_hand,
        competition_level,
        goals,
        coach_id,
        contact_email,
        submitted_at,
        valid_from,
        valid_to,
        is_current
    from students
)

select * from final
