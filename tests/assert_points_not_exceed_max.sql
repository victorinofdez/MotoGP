-- tests/assert_points_not_exceed_max.sql
select
    result_id,
    rider_id,
    session_id,
    year,
    points
from {{ ref('stg_result') }}
where points > 25