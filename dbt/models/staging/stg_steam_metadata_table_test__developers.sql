{{
    config(
        materialized='view'
    )
}}
with 

source as (

    select * from {{ source('staging', 'steam_metadata_table_test__developers') }}

),

renamed as (

    select
        _dlt_id
        value,
        _dlt_parent_id,
        _dlt_list_idx,
        

    from source

)

select * from renamed
{% if var('is_test_run', default=true) %}

  limit 10

{% endif %}