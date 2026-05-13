{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

select
    circuit_id,
    circuit_name,
    circuit_nation,
    circuit_place,
    circuit_legacy_id
from {{ ref('stg_circuit') }}