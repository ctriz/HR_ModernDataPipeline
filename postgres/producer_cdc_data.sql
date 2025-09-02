DELETE from hr_txn.departments where department_id = '10';
DELETE from hr_txn.employee_compensation where employee_id = '101';

INSERT INTO hr_txn.departments (department_id, department_code, name, created_at, updated_at)
VALUES (10, 'HR', 'Human Resources', '2025-08-30 10:00:00', '2025-08-30 10:00:00');


INSERT INTO hr_txn.employee_compensation (comp_id,employee_id, base_salary,bonus, effective_from,effective_to)
VALUES (1000,101, 55000,0,INR, '2025-08-30','2099-12-31');

