{{ config(materialized='table') }}

with snapshot_data as (
    select
        ticker,
        sector,
        dbt_valid_from   as valid_from,
        coalesce(dbt_valid_to, '9999-12-31'::timestamp) as valid_to,
        case when dbt_valid_to is null then true else false end as is_current,
        dbt_scd_id       as scd_id
    from {{ ref('dim_company_snapshot') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['ticker', 'valid_from']) }} as company_key,
    ticker,
    sector,
    valid_from,
    valid_to,
    is_current,
    scd_id,
    current_timestamp() as dbt_loaded_at
from snapshot_data