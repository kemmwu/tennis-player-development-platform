-- Test: first_serve_pct must be between 0 and 1 when not null
select
    match_id,
    first_serve_pct
from {{ ref('stg_match_stats') }}
where first_serve_pct is not null
  and (first_serve_pct < 0 or first_serve_pct > 1)
  