{% snapshot snap_rider_team %}

{{
    config(
        target_schema           = 'SNAPSHOTS',
        target_database         = env_var('DBT_ENVIRONMENTS', 'FAIL') ~ '_MOTOGP_SILVER_DB',
        strategy                = 'check',
        unique_key              = 'rider_team_id',
        check_cols              = ['team_id', 'category_id', 'rider_in_grid'],
        invalidate_hard_deletes = true
    )
}}

select * from {{ ref('stg_rider_team_season') }}

{% endsnapshot %}