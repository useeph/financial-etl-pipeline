{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'stock_prices') }}
),

cleaned as (
    select
        date::date                              as price_date,
        upper(trim(ticker))::varchar(10)        as ticker,
        open::float                             as open_price,
        high::float                             as high_price,
        low::float                              as low_price,
        close::float                            as close_price,
        adj_close::float                        as adj_close_price,
        volume::number(20, 0)                   as volume,
        extracted_at                            as source_extracted_at,
        load_date                               as warehouse_loaded_date,
        source_file                             as source_file_path
    from source
    where close is not null
      and ticker is not null
      and ticker != ''
      and date is not null
),

deduplicated as (
    -- For each (ticker, price_date) pair, keep only the latest extract.
    -- This handles the case where the same date appears in multiple daily snapshots.
    select *
    from cleaned
    qualify row_number() over (
        partition by ticker, price_date
        order by source_extracted_at desc, warehouse_loaded_date desc
    ) = 1
)

select * from deduplicated