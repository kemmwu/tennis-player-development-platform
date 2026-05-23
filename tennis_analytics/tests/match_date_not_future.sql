-- Test: match_date cannot be in the future
select
    match_id,
    match_date
from {{ ref('stg_match_stats') }}
where match_date > current_date()
