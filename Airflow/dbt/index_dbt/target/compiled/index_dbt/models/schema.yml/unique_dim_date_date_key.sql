
    
    

select
    date_key as unique_field,
    count(*) as n_records

from FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.dim_date
where date_key is not null
group by date_key
having count(*) > 1


