with source as (
    select * from {{ ref('player_name_mapping') }}
)

select
    trim(lower(alias))              as alias,
    trim(lower(canonical_player_id)) as canonical_player_id,
    alias_source,
    current_timestamp()             as created_at
from source
