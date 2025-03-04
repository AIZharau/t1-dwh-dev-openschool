/*
  Задание R5.1
Скопируйте в GreenPlum таблицу payments из базы t1_dwh_potok3_datasandbox 
в собственную базу и схему, созданные ранее. Создайте две копии таблицы 
payments_compressed_row и payments_compressed_columnar.  
Используйте документацию при построении таблиц.
 */

create database if not exists dwh_4_t1_zharau;

create schema if not exists payments_task_5;

set search_path to payments_task_5;

create extension dblink;

/*Таблица payments_compressed_row:
 - тип сжатия ZTSD, 
 - уровень сжатия 7,
 - строчный тип хранения.
*/
create table payments_compressed_row
with (
	appendoptimized=true,
    orientation=row,
	compresstype=ZSTD,
	compresslevel=7
)
as 
select * 
from dblink(
	'host=10.10.144.37 dbname=t1_dwh_potok3_datasandbox user=gpadmin password=gparray', 
            'SELECT * FROM payments_and_loans.payments'
) as t1(payment_id int4,
		loan_id int4,
		payment_date date,
		amount numeric,
		create_at timestamp,
		update_at timestamp);

/* Таблица payments_compressed_columnar:
 - тип сжатия RLE, 
 - уровень сжатия 4,
 - колоночный тип хранения. 
 */
create table payments_compressed_columnar
with (
	appendoptimized=true,
	orientation=column,
	compresstype=RLE_TYPE,
   	compresslevel=4
)
as 
select * 
from dblink(
	'host=10.10.144.37 dbname=t1_dwh_potok3_datasandbox user=gpadmin password=gparray', 
            'SELECT * FROM payments_and_loans.payments'
) as t1(payment_id int4,
		loan_id int4,
		payment_date date,
		amount numeric,
		create_at timestamp,
		update_at timestamp);
		
alter schema payments_task_5 rename to payments_and_loans;
set search_path to payments_and_loans;

create table loans
as 
select * 
from dblink(
	'host=10.10.144.37 dbname=t1_dwh_potok3_datasandbox user=gpadmin password=gparray', 
            'SELECT * FROM payments_and_loans.loans'
) as t1(loan_id int4,
		customer_id int4,
		product_id int4,
		amount numeric,
		start_date date,
		end_date date,
		status varchar(150),
		created_at timestamp,
		updated_at timestamp);

create table loans_n (like loans) distributed randomly;
insert into loans_n select * from loans limit 1000;