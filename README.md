# Globepay - DBT Project


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
<img width="1905" height="875" alt="image" src="https://github.com/user-attachments/assets/61aca240-29c3-4967-ab58-256154414b6b" />


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

<img width="397" height="65" alt="image" src="https://github.com/user-attachments/assets/a7d218e7-f1cc-41a6-9fae-a717a0b5c263" />

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
