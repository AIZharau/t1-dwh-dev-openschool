/* Задание второе (S7.1 сквозной проект)
Реализовать функцию по инициализирующей загрузке данных из источника. 
Должны загружаться с временным окном 2 года.
Формат ответа: SQL-скрипт 
*/
	
create or replace function core.initial_load_fact_transactions()
returns text as $$
declare
    v_rows_inserted int;
    v_start_time timestamp := clock_timestamp();
    v_first_date date;
    v_last_date date;
begin
    -- Дата начала и окончания первых двух лет
    select MIN(transaction_date), MAX(transaction_date)
    into v_first_date, v_last_date
    from t1_dwh_potok3_accounts.transactions
    where transaction_date <= (SELECT MIN(transaction_date) + interval '2 years'
    from t1_dwh_potok3_accounts.transactions);
   
    -- Вставка данных за 2 года
    insert into core.fact_transactions (
        transaction_id,
        analytic_account_key,
        transaction_date,
        transaction_type_key,
        amount,
        transaction_count,
        created_at,
        updated_at
    )
    select
        t.transaction_id,
        da.analytic_account_key,
        t.transaction_date::date,
        dt.transaction_type_key,
        t.amount,
        1 as transaction_count,
        t.created_at,
        t.updated_at
    from
        t1_dwh_potok3_accounts.transactions t
    inner join
        core.dim_analytic_accounts da 
        on t.analytic_account_number = da.analytic_account_number
    inner join
        core.dim_transaction_types dt 
        on t.transaction_type = dt.transaction_type
    where
        t.transaction_date >= current_date - interval '2 years';

    -- Количество вставленных строк
    get diagnostics v_rows_inserted = ROW_COUNT;

    -- Возврат
    return 'Initial loading completed. ' || 
           'Rows inserted: ' || v_rows_inserted || ', ' ||
           'Execution time: ' || (clock_timestamp() - v_start_time);
end;
$$ language plpgsql;

select core.initial_load_fact_transactions();

/* Задание третье (S8.1 сквозной проект)
Реализовать функцию по инкрементальной загрузке данных из источника. 
Функция должна загружать только новые записи, т.е. которых в хранилище еще не было.
Формат ответа: SQL-скрипт
*/
set search_path to core;

create or replace function core.incremental_load_fact_transactions()
returns text as $$
declare
    v_rows_inserted int;
    v_start_time timestamp := clock_timestamp();
    v_last_load_date timestamp;
    v_load_date timestamp := clock_timestamp();
begin
    -- Дата последней загрузки
    select last_load_date
    into v_last_load_date
    from core.core_last_load_date
    where table_name = 'fact_transactions';

	
    -- Вставка
    insert into core.fact_transactions (
        transaction_id,
        analytic_account_key,
        transaction_date,
        transaction_type_key,
        amount,
        transaction_count,
        created_at,
        updated_at
    )
    select
        t.transaction_id,
        da.analytic_account_key,
        t.transaction_date::date,
        dt.transaction_type_key,
        t.amount,
        1 as transaction_count,
        t.created_at,
        t.updated_at
    from
        t1_dwh_potok3_accounts.transactions t
    inner join
        core.dim_analytic_accounts da 
        on t.analytic_account_number = da.analytic_account_number
    inner join
        core.dim_transaction_types dt 
        on t.transaction_type = dt.transaction_type
    where
        t.created_at >= v_last_load_date 
        or t.updated_at >= v_last_load_date;

    -- Количество вставленных строк
    get diagnostics v_rows_inserted = ROW_COUNT;

    -- Обновление даты последней загрузки
    update core.core_last_load_date
    set last_load_date = v_load_date
    where table_name = 'fact_transactions';
  
    -- Если запись не найдена, вставляем новую
    if not found then
   		insert into core.core_last_load_date (table_name, last_load_date)
    	values ('fact_transactions', v_load_date);
	end if;
  
    -- Возврат
    return 'Incremental loading completed. ' || 
           'Rows inserted: ' || v_rows_inserted || ', ' ||
           'Execution time: ' || (clock_timestamp() - v_start_time);
end;
$$ language plpgsql;

select core.incremental_load_fact_transactions();