-- Each ticker should have exactly one current row
select ticker, count(*) as current_count
from {{ ref('dim_company') }}
where is_current = true
group by ticker
having count(*) != 1