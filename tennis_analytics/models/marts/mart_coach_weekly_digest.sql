{{
    config(
        materialized='incremental',
        unique_key='digest_id'
    )
}}

with scores as (
    select * from {{ ref('mart_player_development_score') }}
),

{% if is_incremental() %}
-- Only process weeks not already in the table
filtered_scores as (
    select * from scores
    where score_week > (select max(week_start) from {{ this }})
),
{% else %}
filtered_scores as (
    select * from scores
),
{% endif %}

with_trend as (
    select
        player_id,
        player_sk,
        score_week,
        development_score,
        matches_played,
        matches_won,
        sessions_completed,
        avg_shots_per_hour,

        -- Score trend vs prior week
        lag(development_score) over (
            partition by player_id
            order by score_week
        )                                               as prior_week_score,

        development_score - lag(development_score) over (
            partition by player_id
            order by score_week
        )                                               as score_change

    from filtered_scores
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['player_id', 'score_week']) }}
                                                        as digest_id,
        player_id,
        player_sk,
        score_week                                      as week_start,
        development_score,
        prior_week_score,
        score_change,

        case
            when score_change > 2  then 'improving'
            when score_change < -2 then 'declining'
            else 'stable'
        end                                             as score_trend,

        matches_played,
        matches_won,
        sessions_completed,
        avg_shots_per_hour,
        current_timestamp()                             as calculated_at

    from with_trend
)

select * from final
