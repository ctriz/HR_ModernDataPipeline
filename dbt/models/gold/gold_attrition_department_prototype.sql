{{ config(materialized='table',file_format='delta',location='file:///C:/UpScale/HRProject/HRDataPipeline/analytics/delta/gold/gold_attrition_department_prototype') }}

WITH emp AS (
    SELECT
        e.employee_id,
        e.department_id,
        e.updated_at,
        e.is_deleted,
        YEAR(e.updated_at) AS year,
        MONTH(e.updated_at) AS month
    FROM delta.`file:///C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/employees` e
),

agg AS (
    SELECT
        d.department_id,
        d.name AS department_name,
        emp.year,
        emp.month,
        COUNT(CASE WHEN emp.is_deleted = true THEN 1 END) AS attrition_count,
        COUNT(*) AS total_employees
    FROM emp
    JOIN delta.`file:///C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/departments` d
      ON emp.department_id = d.department_id
     AND d.is_deleted = false
    GROUP BY d.department_id, d.name, emp.year, emp.month
)

SELECT
    department_id,
    department_name,
    year,
    month,
    attrition_count,
    total_employees,
    COALESCE(attrition_count / NULLIF(total_employees, 0), 0) AS attrition_rate

FROM agg
ORDER BY year, month, department_name
