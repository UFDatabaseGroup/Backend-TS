insert into timestamp (TIMESTAMP_ID, YEAR, MONTH, DAY)
with timestamp_ids as (
    select distinct timestamp_id, (to_date('1970-01-01', 'YYYY-MM-DD') + numtodsinterval(timestamp_id, 'SECOND')) as ts_date
    from covid_data
    order by timestamp_id asc
)
select timestamp_id, extract(year from ts_date), extract(month from ts_date), extract(day from ts_date)
from timestamp_ids

