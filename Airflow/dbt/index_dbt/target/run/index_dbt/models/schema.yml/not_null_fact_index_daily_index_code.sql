
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select index_code
from FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.fact_index_daily
where index_code is null



  
  
      
    ) dbt_internal_test