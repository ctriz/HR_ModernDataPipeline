
  
    
        create or replace table default_gold.gold_attrition_prototype
      
      
    using delta
      
      
      
      
      
    location 'file:///C:/UpScale/HRProject/HRDataPipeline/analytics/delta/gold/gold_attrition_prototype'
      

      as
      

WITH base AS (
    SELECT
        employee_id,
        department_id,
        job_id,
        location_id,
        updated_at,
        is_deleted,
        YEAR(updated_at) AS year,
        MONTH(updated_at) AS month
    FROM delta.`file:///C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/employees`
)

SELECT
    year,
    month,
    COUNT(CASE WHEN is_deleted = true THEN 1 END) AS attrition_count,
    COUNT(*) AS total_employees,
    COALESCE(
    COUNT(CASE WHEN is_deleted = true THEN 1 END) / NULLIF(COUNT(*), 0),
    0
    ) AS attrition_rate
FROM base
GROUP BY year, month
ORDER BY year, month
  