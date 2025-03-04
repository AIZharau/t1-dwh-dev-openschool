/*
  Задание R5.1
Скопируйте в GreenPlum таблицу payments из базы t1_dwh_potok3_datasandbox 
в собственную базу и схему, созданные ранее. Создайте две копии таблицы 
payments_compressed_row и payments_compressed_columnar.  
Используйте документацию при построении таблиц.
 */

select current_database();
set search_path to payments_and_loans;
select current_schema();

create table payments (
	payment_id serial4 not null,
	loan_id int4 null,
	payment_date date null,
	amount numeric null,
	created_at timestamp null,
	updated_at timestamp null,
	constraint payments_pkey primary key (payment_id)
)
distributed by (payment_id);

insert into payments
select * 
from dblink(
    'host=10.10.144.37 dbname=t1_dwh_potok3_datasandbox user=gpadmin password=gparray', 
    'SELECT * FROM payments_and_loans.payments'
) as t1(
    payment_id int4,
    loan_id int4,
    payment_date date,
    amount numeric,
    created_at timestamp,  
    updated_at timestamp  
);

/*
    Таблица payments_compressed_row:
 - должна иметь тип сжатия ZTSD, 
 - уровень сжатия 7,
 -  строчный тип хранения.
*/
create table payments_compressed_row (
	like payments
)
with (
	appendoptimized=true,
    orientation=row,
	compresstype=ZSTD,
	compresslevel=7
)
distributed by (payment_id);

insert into payments_compressed_row
select * from payments;

/*
    Таблица payments_compressed_columnar:
 - тип сжатия RLE, 
 - уровень сжатия 4,
 - колоночный тип хранения. 
*/
create table payments_compressed_columnar (
	like payments
)
with (
	appendoptimized=true,
	orientation=column,
	compresstype=RLE_TYPE,
   	compresslevel=4
)
distributed by (payment_id);
		
insert into payments_compressed_columnar 
select * from payments;