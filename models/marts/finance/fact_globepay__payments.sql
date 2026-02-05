{{
    config(
        materialized='incremental',
        on_schema_change='sync_all_columns',
        unique_key='transaction_id',
        tags=['payments']
    )
}}

with 

payments as (

    select 
        transaction_id
        , order_id
        , created_at
        , country
        , currency
        , state
        , amount
        , rates
        , usd_rate
        , is_cvv_provided
        , is_accepted
        , is_declined   
    
    from {{ ref('stg_globepay__payments') }}

    {% if is_incremental() %}

    where created_at >= (select dateadd(day, -2, max(created_at)) from {{ this }})

    {% endif %}

), 

chargebacks as (

    select 
        transaction_id
        , is_chargeback 
    
    from {{ ref('stg_globepay__chargebacks') }}


),

joined as (

    select
        p.transaction_id
        , p.order_id
        , p.created_at
        , p.country
        , p.currency
        , p.amount
        , p.rates
        , p.usd_rate
        , p.is_cvv_provided
        , p.is_accepted
        , p.is_declined
        , c.is_chargeback as is_chargeback -- field would be null if the transaction doesn't have chargeback data, instead of false

    from payments as p

    left join chargebacks as c
    on p.transaction_id = c.transaction_id

)

select * from joined
