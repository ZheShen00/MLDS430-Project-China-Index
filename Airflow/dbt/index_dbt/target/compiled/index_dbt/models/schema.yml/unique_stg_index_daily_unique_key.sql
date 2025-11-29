
    
    

select
    unique_key as unique_field,
    count(*) as n_records

from FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.stg_index_daily
where unique_key is not null
group by unique_key
having count(*) > 1


