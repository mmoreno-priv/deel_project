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
        , date_time
        --, status
        --, source
        , ref
        , state
        , cvv_provided
        , amount
        , country
        , currency
        , rates
    from {{ source('main','globepay_payments') }}

    --where source ='GLOBALPLAY'
    
    -- macro to limit to one day of data in case of development env ?
    --{% if target.name == 'dev' %}
    --and date_time >= current_date - 1
    --{% endif %}

    qualify row_number() over (partition by external_ref order by date_time desc) = 1

),

renamed as (

    select
        -- ids
        external_ref::varchar(64) as transaction_id
        , ref::varchar(64) as order_id

        -- date fields
        , date_time::timestamptz as created_at
 
        -- payments fields
        , country::varchar(2) as country
        , currency::varchar(3) as currency
        , lower(state)::varchar(50) as state
        , amount::float as amount
        , rates
        -- parse USD rate from JSON. NOTE: Only doing this for USD since it's part of the questions
        , json_extract(rates, '$.USD')::float as usd_rate
 
        -- flags
        , cvv_provided::boolean as is_cvv_provided
        , case
            when lower(state) = 'accepted' then true
             else false
         end as is_accepted
        , case
             when lower(state) = 'declined' then true
             else false
         end as is_declined


    from source

)

select * from renamed
