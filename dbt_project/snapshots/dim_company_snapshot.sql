{% snapshot dim_company_snapshot %}

{{
    config(
      target_schema='ANALYTICS_SNAPSHOTS',
      unique_key='ticker',
      strategy='check',
      check_cols=['sector'],
      invalidate_hard_deletes=True,
    )
}}

select
    ticker,
    case
        when ticker in ('AAPL','MSFT','NVDA','GOOGL','GOOG','META','AMZN','TSLA','AVGO','ORCL','CRM','ADBE','CSCO','AMD','INTC','IBM','QCOM','TXN','NOW','INTU')
            then 'Technology'
        when ticker in ('JPM','BAC','WFC','C','GS','MS','AXP','BLK','SCHW','V','MA','PYPL','COF','USB','PNC')
            then 'Financials'
        when ticker in ('UNH','JNJ','PFE','LLY','MRK','ABBV','TMO','ABT','DHR','BMY','CVS','MDT','AMGN','GILD')
            then 'Healthcare'
        when ticker in ('XOM','CVX','COP','SLB','EOG','PSX','MPC','VLO','OXY','HES')
            then 'Energy'
        when ticker in ('WMT','HD','LOW','TGT','COST','MCD','SBUX','NKE','TJX','DG')
            then 'Consumer'
        else 'Other'
    end as sector
from (
    select distinct ticker
    from {{ ref('stg_stock_prices') }}
)

{% endsnapshot %}