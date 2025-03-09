/* Лекция 7: Практическое задание 
   Задание первое
Реализовать функцию по инкрементальному обновлению данных из таблицы payments
тестовой базы данных. 
Скрипт должен обновлять данные за последние две недели от текущей даты, 
т.к. обновления могут приходить в таблицу в течении этого периода. 
Также из таблицы payments необходимо забирать и новые данные. 
Обновлять необходимо информацию только по тем кредитам, которые находятся в 
статусе «Открыт» на текущий момент. Допускается использовать иные таблицы 
из тестовой базы данных.
Формат ответа: SQL-скрипт
*/
-- Создание и настройка postgres_jdbc_srv выполнены в рамках задания 6.1
create foreign table payments_and_loans.payments_extended (
    payment_id int4,
    loan_id int4,
    payment_date date,
    amount numeric,
    created_at timestamp,
    updated_at timestamp
)
server postgres_jdbc_srv
options (
    schema_name 'payments_and_loans',
    table_name 'payments'
);

create or replace function initial_loading_update_payments_fwd()
returns void as $$
begin
    -- Создание временной таблицы для хранения актуальных данных
    create temp table temp_recent_payments on commit drop as
    select
        p.payment_id,
        p.loan_id,
        p.payment_date,
        p.amount,
        p.created_at,
        p.updated_at
    from
        payments_and_loans.payments_extended p
    inner join
        payments_task_5_test.loans l 
        on p.loan_id = l.loan_id
    where
        (p.payment_date >= current_date - interval '14 days' or
         p.updated_at >= current_date - interval '14 days')
        and l.status = 'Открыт';

    -- Удаление устаревших записей из целевой таблицы
    delete from
        payments_and_loans.payments target
    using
        temp_recent_payments src
    where
        target.payment_id = src.payment_id;

    -- Вставка
    insert into payments_and_loans.payments
    select * from temp_recent_payments;

end;
$$ language plpgsql;

select initial_loading_update_payments_fwd();