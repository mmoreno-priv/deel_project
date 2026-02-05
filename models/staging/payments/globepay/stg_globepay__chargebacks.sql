{{ 
  config(
    materialized='table',
    tags=['source','payments']
  ) 
}}

with

source as (

    select 
        external_ref
        --, status
        --, source
        , chargeback
    from {{ source('main','globepay_chargebacks') }}
    
    -- macro to limit in case of development env
    --{% if target.name == 'dev' %}
    --limit 100
    --{% endif %}


),

renamed as (

    select
        external_ref::varchar(64) as transaction_id
        , chargeback::boolean as is_chargeback

    from source

)

select * from renamed
