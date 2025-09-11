{{ config(materialized='table') }}

SELECT
    attrition_year,
    attrition_month,
    SUM(attrition_count) AS attrition_count,
    SUM(headcount) AS headcount,
    COALESCE(SUM(attrition_count) / NULLIF(SUM(headcount), 0), 0) AS attrition_rate
FROM {{ source('silver', 'fact_employee_attrition_events') }}
GROUP BY attrition_year, attrition_month
ORDER BY attrition_year, attrition_month
