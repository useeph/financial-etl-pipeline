{{ config(
    materialized='table',
    cluster_by=['price_date', 'ticker']
) }}

with prices as (
    select * from {{ ref('stg_stock_prices') }}
),

companies as (
    -- All historical versions of each ticker
    select company_key, ticker, valid_from, valid_to
    from {{ ref('dim_company') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.ticker', 'p.price_date']) }} as price_key,
        c.company_key,
        p.price_date,
        p.ticker,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.adj_close_price,
        p.volume,
        round(p.close_price - p.open_price, 4)                                  as daily_change,
        round(((p.close_price - p.open_price) / nullif(p.open_price, 0)) * 100, 4) as daily_pct_change,
        round((p.high_price - p.low_price), 4)                                  as daily_range,
        p.close_price * p.volume                                                as dollar_volume,
        p.source_extracted_at,
        p.warehouse_loaded_date
    from prices p
    left join companies c
        on p.ticker = c.ticker
        and p.price_date >= c.valid_from::date
        and p.price_date < c.valid_to::date
)

select * from joined