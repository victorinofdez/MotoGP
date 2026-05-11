with
    source as (
        select * from {{ source('raw', 'categories') }}
),
renamed as (
    select 
        id                                          as category_id,
        name,
        legacy_id                                   as legacy_id,     
        case name
            when 'MotoGP™' then 'Premier'
            when '500cc'   then 'Premier'
            when 'Moto2™'  then 'Intermediate'
            when '250cc'   then 'Intermediate'
            when 'Moto3™'  then 'Junior'
            when '125cc'   then 'Junior'
            else 'Historical'
        end                                         as class_tier
    from source
    qualify row_number() over (
            partition by id
            order by _ingested_at desc
    ) = 1
)

select * from renamed