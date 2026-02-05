# Globepay - DBT Project

<img width="1044" height="199" alt="image" src="https://github.com/user-attachments/assets/271e47dc-b8f6-43f0-b59b-fd73c6fff3f4" />

This project ingests and models payment and chargeback data from Globepay. It uses a staging → fact layer approach following dbt best practices, making it easier to analyze payment performance over time.

## Preliminary Data Exploration

Two CSV files were provided:

### 1. Globepay Acceptance Report
- Each row corresponds to a single attempted payment transaction.
- Fields include:
  - `external_ref`: Unique transaction ID
  - `ref`: Event ID (starts with `evt_`)
  - `date_time`: Timestamp of the transaction
  - `state`: Status of the transaction (`ACCEPTED` or `DECLINED`)
  - `cvv_provided`: Boolean flag if CVV was provided
  - `amount`: Charged amount
  - `country`: ISO country code
  - `currency`: ISO currency code
  - `rates`: JSON containing exchange rates to USD and other currencies

### 2. Globepay Chargeback Report
- Contains chargeback information for transactions.
- Fields include:
  - `external_ref`: Transaction ID (links to acceptance report)
  - `chargeback`: Boolean flag indicating chargeback

**Data note:** `external_ref` is the transaction identifier linking both files.

## Model architecture

Followed this approach:

1. Staging Layer (stg_)
  - Cleans raw CSV data
  - Standardizes column names and types
  - Parses JSON columns like rates
  - Staging models: stg_globepay__payments and stg_globepay__chargebacks

2. Marts
  - Joins and enriches staging tables
  - Adds derived flags
  - Supports incremental loads for cost optimization
  - Fact model: fact_globepay__payments - this model was created to answer the 3 questions below

The sources (.CSV files) were added to the data folder and populated in dbt as seeds.

## Lineage graph

CSV files → Staging tables (clean & standardize) → Fact table (enriched & analytics-ready)

<img width="1122" height="383" alt="image" src="https://github.com/user-attachments/assets/62790284-a192-4756-a9e9-bc65b5626ce3" />


---



## Questions Part 2
### 1. What is the acceptance rate over time?
```sql
with cte as (
select
    date(created_at) as date
    , sum(case when is_accepted then 1 else 0 end) as accepted_transactions
    , count(distinct transaction_id) as total_transactions
from fact_globepay__payments
group by 1
)
select
    date
    , round(100* accepted_transactions / total_transactions, 2) as acceptance_rate
from cte
order by 1 desc
```
<img width="1905" height="875" alt="image" src="https://github.com/user-attachments/assets/58abb97f-491d-42a5-9bcf-ea73e07def79" />


### 2. List the countries where the amount of declined transactions went over $25M
```sql
with cte as (
select
    country
    , sum(amount * usd_rate) as total_amount_usd
from fact_globepay__payments
where is_declined is true
group by 1
)
select
   distinct country
from cte
where total_amount_usd > 25000000
```

<img width="397" height="65" alt="image" src="https://github.com/user-attachments/assets/b2b536fa-5c48-4ab0-b81f-0735d98c2ed2" />


Countries that have an amount of declined transactions over 25M USD are: Canada (CA), United States of America (US) and United Arab Emirates (AE).

### 3. Which transactions are missing chargeback data?
A transaction is considered “missing chargeback data” if there is no corresponding record in the chargebacks dataset.
This is different from FALSE chargebacks, which explicitly indicate that a transaction had no chargeback.

```sql
select
    transaction_id
from fact_globepay__payments
where is_chargeback is null
```
Results: 0 rows. All transactions have chargeback information.

