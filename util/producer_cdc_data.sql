set schema 'hr_txn';
DELETE from hr_txn.departments where department_id = '10';
DELETE from hr_txn.employee_compensation where employee_id = '401';

INSERT INTO hr_txn.departments (department_id, department_code, name, created_at, updated_at)
VALUES (10, 'HR', 'Human Resources', '2025-08-30 10:00:00', '2025-08-30 10:00:00');


INSERT INTO hr_txn.employee_compensation (comp_id,employee_id,base_salary,bonus,currency,effective_from,effective_to)
VALUES (1000,401, 55000,0,'USD','2025-08-30','2099-12-31');

SELECT * from employee_compensation;

SELECT * from employees;