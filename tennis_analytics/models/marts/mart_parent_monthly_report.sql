{{
    config(
        materialized='incremental',
        unique_key='report_id'
    )
}}

with matches as (
    select * from {{ ref('fct_match_performance') }}
),

training as (
    select * from {{ ref('fct_training_sessions') }}
),

scores as (
    select * from {{ ref('mart_player_development_score') }}
),

{% if is_incremental() %}
new_months as (
    select distinct date_format(match_date, 'yyyy-MM') as report_month
    from matches
    where date_format(match_date, 'yyyy-MM')
        > (select max(report_month) from {{ this }})
),
{% else %}
new_months as (
    select distinct date_format(match_date, 'yyyy-MM') as report_month
    from matches
),
{% endif %}

monthly_matches as (
    select
        m.player_id,
        date_format(m.match_date, 'yyyy-MM')            as report_month,
        count(*)                                        as matches_played,
        sum(case when m.player_won then 1 else 0 end)   as matches_won,
        round(
            sum(case when m.player_won then 1 else 0 end)
            / nullif(count(*), 0)
        , 4)                                            as win_rate,
        round(avg(m.first_serve_pct), 4)                as avg_first_serve_pct,
        round(avg(m.break_point_conversion), 4)         as avg_break_point_conversion
    from matches m
    inner join new_months nm
        on date_format(m.match_date, 'yyyy-MM') = nm.report_month
    group by
        m.player_id,
        date_format(m.match_date, 'yyyy-MM')
),

monthly_training as (
    select
        t.player_id,
        date_format(t.session_date, 'yyyy-MM')          as report_month,
        count(*)                                        as sessions_completed,
        round(avg(t.total_shots_per_hour), 1)           as avg_shots_per_hour
    from training t
    inner join new_months nm
        on date_format(t.session_date, 'yyyy-MM') = nm.report_month
    group by
        t.player_id,
        date_format(t.session_date, 'yyyy-MM')
),

monthly_scores as (
    select
        player_id,
        date_format(score_week, 'yyyy-MM')              as report_month,
        round(avg(development_score), 2)                as avg_development_score,
        round(max(development_score)
            - min(development_score), 2)                as score_range
    from scores
    group by
        player_id,
        date_format(score_week, 'yyyy-MM')
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['mm.player_id', 'mm.report_month']
        ) }}                                            as report_id,
        mm.player_id,
        mm.report_month,
        mm.matches_played,
        mm.matches_won,
        mm.win_rate,
        mm.avg_first_serve_pct,
        mm.avg_break_point_conversion,
        coalesce(mt.sessions_completed, 0)              as sessions_completed,
        mt.avg_shots_per_hour,
        ms.avg_development_score                        as development_score,
        ms.score_range,
        current_timestamp()                             as calculated_at

    from monthly_matches mm
    left join monthly_training mt
        on  mm.player_id    = mt.player_id
        and mm.report_month = mt.report_month
    left join monthly_scores ms
        on  mm.player_id    = ms.player_id
        and mm.report_month = ms.report_month
)

select * from final
