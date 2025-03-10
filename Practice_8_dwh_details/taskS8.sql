/* Лекция 8: Практическое задание */

/*	Задание S8.1
Реализуйте в рамках сквозного проекта деление на слои данных. 
Каждый слой в сквозном проекте должен иметь свое оригинальное название.
Формат ответа: SQL-скрипты в текстовом документе и схемы/таблицы 
			   в вашем сквозном проекте. 
*/

/* satging layer - реализован в виде t1_dwh_potok3_accounts,
создал копию, чтобы не нарушать последовательность выполнения заданий
*/
-- DDL satging layer 
create schema if not exists staging;
set search_path to staging;

CREATE TABLE stg_transactions (
	transaction_id bigserial NOT NULL,
	analytic_account_number varchar(255) NULL,
	amount numeric(15, 2) NOT NULL,
	transaction_date timestamp NULL,
	transaction_type varchar(255) NULL,
	description varchar(255) NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL,
	CONSTRAINT transactions_pkey PRIMARY KEY (transaction_id)
)
DISTRIBUTED BY (transaction_id);

CREATE TABLE stg_analytic_accounts (
	analytic_account_number varchar(255) NULL,
	organization_id int4 NULL,
	synthetic_account_number varchar(255) NULL,
	balance numeric(15, 2) NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
)
DISTRIBUTED BY (analytic_account_number);

CREATE TABLE stg_synthetic_accounts (
	synthetic_account_number varchar(255) NULL,
	description varchar(255) NULL,
	account_type varchar(255) NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
)
DISTRIBUTED BY (synthetic_account_number);

CREATE TABLE stg_organizations (
	organization_id int4 NULL,
	"name" varchar(255) NULL,
	registration_number varchar(255) NULL,
	address varchar(255) NULL,
	contact_info varchar(255) NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
)
DISTRIBUTED BY (organization_id);

-- Загрузка данных в staging
-- Таблицу для хранения даты последней загрузки данных
create table stg_last_load_date (
    table_name varchar(255) primary key,
    last_load_date timestamp
);

-- Функция инкрементальной загрузки в таблицы staging
create or replace function incremental_load_table(p_table_name varchar)
returns text as $$
declare
    v_rows_inserted int;
    v_start_time timestamp := clock_timestamp();
    v_last_load_date timestamp;
    v_sql text;
begin
    -- Дата последней загрузки
    select last_load_date
    into v_last_load_date
    from staging.stg_last_load_date
    where table_name = p_table_name;

    -- Если дата последней загрузки не найдена, загружаем все данные
    if v_last_load_date is null then
        v_last_load_date := '1970-01-01'::timestamp;
    end if;

    -- SQL-запрос для загрузки данных
    v_sql := format(
        'INSERT INTO staging.stg_%I ' ||
        'SELECT * ' ||
        'FROM t1_dwh_potok3_accounts.%I ' ||
        'WHERE created_at >= $1 OR updated_at >= $1',
        p_table_name, p_table_name
    );

    -- Выполнение SQL-запроса
    execute v_sql using v_last_load_date;

    -- Обновление даты последней загрузки
    update staging.stg_last_load_date
    set last_load_date = clock_timestamp()
    where table_name = p_table_name;
   
    -- Если запись не найдена
	if not found then
		insert into staging.stg_last_load_date (table_name, last_load_date)
		values (p_table_name, clock_timestamp());
	end if;

    -- Количество вставленных строк
    get diagnostics v_rows_inserted = ROW_COUNT;

    -- Вывод результата
    return format('Incremental load for table %I completed. ' || 
                  'Rows inserted: %s, ' ||
                  'Execution time: %s',
                  p_table_name, v_rows_inserted, clock_timestamp() - v_start_time);
end;
$$ language plpgsql;

select incremental_load_table('transactions');
select incremental_load_table('analytic_accounts');
select incremental_load_table('organizations');
select incremental_load_table('synthetic_accounts');


/* core layer */  -- создан ранее 

-- Таблица для хранения даты последней загрузки данных
create table core.core_last_load_date (
    table_name varchar(255) primary key,
    last_load_date timestamp
);

-- Вставка данных в таблицы вручную
-- Синтетические счета
insert into core.dim_synthetic_accounts (
    synthetic_account_number,
    description,
    account_type,
    created_at,
    updated_at
)
select distinct
    s.synthetic_account_number,
    s.description,
    s.account_type,
    s.created_at,
    s.updated_at
from
    staging.stg_synthetic_accounts s
where
    s.synthetic_account_number not in (
        select synthetic_account_number 
        from core.dim_synthetic_accounts
    );

-- Организации
insert into core.dim_organizations (
    organization_id,
    name,
    registration_number,
    address,
    contact_info,
    created_at,
    updated_at
)
select distinct
    o.organization_id,
    o.name,
    o.registration_number,
    o.address,
    o.contact_info,
    o.created_at,
    o.updated_at
from
    staging.stg_organizations o
where
    o.organization_id not in (
        select organization_id 
        from core.dim_organizations
    );
   
-- Типы транзакций
insert into core.dim_transaction_types (transaction_type, description)
select distinct
    t.transaction_type,
    'Auto-added from staging' as description
from
    staging.stg_transactions t
where
    t.transaction_type not in (
        select transaction_type 
        from core.dim_transaction_types
    );

-- Аналитические счета
insert into core.dim_analytic_accounts (
    analytic_account_number,
    organization_id,
    synthetic_account_number,
    balance,
    created_at,
    updated_at
)
select distinct
    a.analytic_account_number,
    a.organization_id,
    a.synthetic_account_number,
    a.balance,
    a.created_at,
    a.updated_at
from
    staging.stg_analytic_accounts a
where
    a.analytic_account_number not in (
        select analytic_account_number 
        from core.dim_analytic_accounts
    );
   
-- Транзакции
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
    1 as transaction_count,  -- Каждая строка = одна транзакция
    t.created_at,
    t.updated_at
from
    staging.stg_transactions t
inner join
    core.dim_analytic_accounts da 
    on t.analytic_account_number = da.analytic_account_number
inner join
    core.dim_transaction_types dt 
    on t.transaction_type = dt.transaction_type
where
    t.transaction_id not in (
        select transaction_id 
        from core.fact_transactions
    );

/* mart layer */
-- DDL mart layer
create schema if not exists mart;
   
create table mart.mart_last_load_date (
    table_name varchar(255) primary key,
    last_load_date timestamp
);

-- Ежемесячные транзакции по организациям
create table mart.mart_monthly_transactions (
    analytic_account_key int not null,
    transaction_month date not null,
    total_amount numeric(15, 2) not null,
    transaction_count int not null,
    load_date date not null,
    update_date date not null,
    constraint mart_monthly_transactions_pkey primary key (analytic_account_key, transaction_month)
)
distributed by (analytic_account_key);

-- Балансы организаций
create table mart.mart_organization_balances (
    organization_key int,
    total_balance numeric(15,2),
    last_updated timestamp
)
distributed by (organization_key);

-- Заполнение витрины mart_organization_balances
create or replace function mart.incremental_update_organization_balances()
returns text as $$
declare
    v_rows_updated int;
    v_rows_inserted int;
    v_start_time timestamp := clock_timestamp();
    v_last_load_date timestamp;
    v_load_date timestamp := clock_timestamp();
begin
    -- Дата последней загрузки
    select last_load_date
    into v_last_load_date
    from mart.mart_last_load_date
    where table_name = 'mart_organization_balances';

    -- Если дата последней загрузки не найдена, загрузить все данные
    if v_last_load_date is null then
        v_last_load_date := '1970-01-01'::timestamp;
    end if;

    -- Обновление записей
    with updated_balances as (
        select 
            o.organization_key,
            SUM(a.balance) AS total_balance
        from
            core.dim_analytic_accounts a
        inner join
            core.dim_organizations o 
            on a.organization_id = o.organization_id
        where
            a.updated_at >= v_last_load_date
        group by
            o.organization_key
    )
    update mart.mart_organization_balances  mob
    set
        total_balance = ub.total_balance,
        last_updated = v_load_date
    from
        updated_balances ub
    where
        mob.organization_key = ub.organization_key;

    -- Количество обновленных строк
    get diagnostics v_rows_updated = ROW_COUNT;

    -- Вставка
    insert into mart.mart_organization_balances (
        organization_key,
        total_balance,
        last_updated
    )
    select
        o.organization_key,
        SUM(a.balance) as total_balance,
        v_load_date as last_updated
    from
        core.dim_analytic_accounts a
    inner join
        core.dim_organizations o 
        on a.organization_id = o.organization_id
    where
        a.updated_at >= v_last_load_date
        and o.organization_key not in (
            select organization_key 
            from mart.mart_organization_balances
        )
    group by
        o.organization_key;

    -- Количество вставленных строк
    get diagnostics v_rows_inserted = ROW_COUNT;

    -- Обновить дату последней загрузки
    update mart.mart_last_load_date
    set last_load_date = v_load_date
    where table_name = 'mart_organization_balances';

    -- Если запись не найдена, вставить новую
    if not found then
        insert into mart.mart_last_load_date (table_name, last_load_date)
        values ('mart_organization_balances', v_load_date);
    end if;

    -- Возврт
    return 'Incremental update completed. ' || 
           'Rows updated: ' || v_rows_updated || ', ' ||
           'Rows inserted: ' || v_rows_inserted || ', ' ||
           'Execution time: ' || (clock_timestamp() - v_start_time);

end;
$$ language plpgsql;

select mart.incremental_update_organization_balances();

/*	Задание S8.2
Реализуйте накопительную витрину для своего сквозного проекта. 
Витрина должна иметь системный атрибут/атрибуты, определяющий дату обновления витрины.
Формат ответа: SQL-скрипты в текстовом документе и таблицы в схеме данных слоя витрин.
*/

drop table mart.mart_transactions_history
-- Обновлнная витрина: ежемесячные транзакции по организациям
create table mart.mart_transactions_history (
    transaction_id bigint not null,
    analytic_account_key int not null,
    transaction_date date not null,
    transaction_type_key int not null,
    amount numeric(15, 2) not null,
    load_date date not null,
    update_date date not null
)
with (
    appendonly=true,
    orientation=column,
    compresstype=zstd,
    compresslevel=5
)
distributed by (transaction_id)
partition by range (load_date) 
(
    start ('2020-01-01'::date) end ('2026-01-01'::date) every ('1 mon'::interval)
);

create or replace function mart.update_mart_transactions_history()
returns text as $$
declare
    v_rows_inserted int;
    v_start_time timestamp := clock_timestamp();
    v_load_date date := current_date;
    v_max_load_date date;
begin
	-- Максимальная дата загрузки
	select coalesce(max(load_date), '1970-01-01')
	into v_max_load_date
	from mart.mart_transactions_history;

    -- Вставка новых данных
	insert into mart.mart_transactions_history (
	    transaction_id,
	    analytic_account_key,
	    transaction_date,
	    transaction_type_key,
	    amount,
	    load_date,
	    update_date
	)
	select
	    t.transaction_id,
	    da.analytic_account_key,
	    t.transaction_date::date,
	    dt.transaction_type_key,
	    t.amount,
	    v_load_date as load_date,
	    v_load_date as update_date
	from
	    staging.stg_transactions t
	inner join
	    core.dim_analytic_accounts da 
	    on t.analytic_account_number = da.analytic_account_number
	inner join
	    core.dim_transaction_types dt 
	    on t.transaction_type = dt.transaction_type
	where
	    t.created_at >= v_max_load_date
	    or t.updated_at >= v_max_load_date;

    -- Обновление существующих записей
    with latest_transaction as (
    	select
    		t.transaction_id,
    		t.amount,
    		row_number() over (partition by t.transaction_id order by t.updated_at desc) as rn
		from 
			staging.stg_transactions t
		inner join 
			core.dim_analytic_accounts da
			on t.analytic_account_number = da.analytic_account_number 
		inner join 
			core.dim_transaction_types dt
			on t.transaction_type = dt.transaction_type
		where
			t.updated_at >= v_max_load_date
	)
    update mart.mart_transactions_history mth
    set
        amount = lt.amount,
        update_date = v_load_date
    from
        latest_transaction lt
    where
        mth.transaction_id = lt.transaction_id
        and mth.load_date = v_load_date
        and lt.rn = 1;

    -- Количество вставленных строк
    get diagnostics v_rows_inserted = ROW_COUNT;

       -- Обновить дату последней загрузки
    update mart.mart_last_load_date
    set last_load_date = v_load_date
    where table_name = 'mart_transactions_history';

    -- Если запись не найдена, вставить новую
    if not found then
        insert into mart.mart_last_load_date (table_name, last_load_date)
        values ('mart_transactions_history', v_load_date);
    end if;
   
    return format('Update mart_transactions_history completed. ' || 
                  'Rows inserted/updated: %s, ' ||
                  'Execution time: %s',
                  v_rows_inserted, clock_timestamp() - v_start_time);
end;
$$ language plpgsql;

select mart.update_mart_transactions_history();