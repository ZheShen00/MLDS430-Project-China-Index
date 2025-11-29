
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_key
from FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.stg_index_daily
where unique_key is null



  
  
      
    ) dbt_internal_test