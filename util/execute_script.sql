set schema 'hr_txn';
delete from employee_compensation;

delete from employees;

delete from departments;

delete from jobs;

delete from locations;


ALTER TABLE hr_txn.employee_compensation
DROP CONSTRAINT employee_compensation_check;

ALTER TABLE hr_txn.employee_compensation
ADD CONSTRAINT employee_compensation_check
CHECK (effective_to IS NULL OR effective_to >= effective_from - INTERVAL '1 day');