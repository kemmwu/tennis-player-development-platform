-- Test: shots_in must be between 0 and 1
select
    session_id,
    shots_in
from {{ ref('stg_training_sessions') }}
where shots_in is not null
  and (shots_in < 0 or shots_in > 1)
  