> [!**Задание  S5.1**]
> Найдите и изучите таблицы из схем t1_dwh_potok3_accounts. Реализуйте на базе таблиц модель данных ядра хранилища в 3НФ (Inmon) или же в 1-2НФ (Kimball) в формате ER-диаграммы. Используйте рекомендации из лекционного материала.
> **Формат ответа**: Текстовый документ.

ранее . . .
[!Выбран вариант второй]
> Реализовать локальное хранилище для информации по аналитическим и балансовым счетам. Информация по счетам обновляется 4 раза в день, забирается из накопленного источника (внешняя БД). Фактические данные – проводки по счетам. => schema: t1_dwh_potok3_accounts


### **Выбор модели данных:**
 **Kimball (1-2НФ) с витринами в формате "звезда"** (Star Schema):
- **Источник данных**: Накопленные данные из внешней БД (OLTP-система).
- **Частота обновлений**: 4 раза в день (ETL-пакеты, а не потоковые обновления).
- **Характер запросов**: Аналитические (агрегации по транзакциям, отчеты по счетам и организациям).
- **Масштаб данных**: Транзакции могут быть объемными, требуется оптимизация для чтения.

### **ER-диаграмма в формате "звезда":**
#### **Таблица фактов (Fact):**
- **`fact_transactions`** (из `transactions`):
  - **Меры**:
    - `amount` (сумма транзакции).
    - `transaction_count` (количество транзакций, если требуется агрегация).
  - **Связи с измерениями**:
    - `analytic_account_key` → `dim_analytic_accounts`.
    - `transaction_date`.
    - `transaction_type_key` → `dim_transaction_types`.
    -  `created_at`,   `updated_at`.

```sql
CREATE TABLE fact_transactions (
    transaction_id bigint,
    analytic_account_key int,
    transaction_date date NOT NULL,
    transaction_type_key int,
    amount numeric(15,2),
    transaction_count int
    created_at timestamp DEFAULT current_timestamp,
	  updated_at timestamp DEFAULT current_timestamp
)
WITH (APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zstd)
DISTRIBUTED BY (analytic_account_key)
PARTITION BY RANGE (date_key) 
(START (20240101) END (20250101) EVERY (INTERVAL '1 month'));
```

---

#### **Таблицы измерений (Dimensions):**
1. **`dim_analytic_accounts`** (из `analytic_accounts` + денормализация):
   - **Поля**:
     - `analytic_account_key` (суррогатный ключ).
     - `analytic_account_number`.
     - `organization_id` (связь с `dim_organizations`).
     - `synthetic_account_number` (связь с `dim_synthetic_accounts`).
     - `balance` (текущий баланс счета).
     - `created_at`, `updated_at`.

   ```sql
   CREATE TABLE dim_analytic_accounts (
       analytic_account_key serial PRIMARY KEY,
       analytic_account_number varchar(255),
       organization_id int,
       synthetic_account_number varchar(255),
       balance numeric(15,2),
       created_at timestamp,
       updated_at timestamp
   )
   DISTRIBUTED BY (analytic_account_key);
   ```

2. **`dim_organizations`** (на базе `organizations`):
   - **Поля**:
     - `organization_key` (суррогатный ключ).
     - `name`, `registration_number`, `address`, `contact_info`.
     - `created_at`, `updated_at`.

   ```sql
   CREATE TABLE dim_organizations (
       organization_key serial PRIMARY KEY,
       organization_id int,
       name varchar(255),
       registration_number varchar(255),
       address varchar(255),
       contact_info varchar(255),
       created_at timestamp,
       updated_at timestamp
   )
   DISTRIBUTED BY (organization_key);
   ```

3. **`dim_synthetic_accounts`** (на базе `synthetic_accounts`):
   - **Поля**:
     - `synthetic_account_key` (суррогатный ключ).
     - `synthetic_account_number`.
     - `description`, `account_type`.
     - `created_at`, `updated_at`.

   ```sql
   CREATE TABLE dim_synthetic_accounts (
       synthetic_account_key serial PRIMARY KEY,
       synthetic_account_number varchar(255),
       description varchar(255),
       account_type varchar(255),
       created_at timestamp,
       updated_at timestamp
   )
   DISTRIBUTED BY (synthetic_account_key);
   ```

4. **`dim_transaction_types`** (справочник типов транзакций):
   - **Поля**:
     - `transaction_type_key` (суррогатный ключ).
     - `transaction_type` (депозит, вывод и т.д.).
     - `description`.

   ```sql
   CREATE TABLE dim_transaction_types (
       transaction_type_key serial PRIMARY KEY,
       transaction_type varchar(255),
       description varchar(255)
   )
   DISTRIBUTED BY (transaction_type_key);
   ```

```
### **Почему денормализация:**
- **`dim_analytic_accounts`** включает поля из `organizations` и `synthetic_accounts`, чтобы избежать JOIN-ов при анализе счетов.
- **`fact_transactions`** содержит только ключи измерений и меры, что упрощает агрегации.
- **Суррогатные ключи** (`analytic_account_key`, `organization_key` и т.д.) улучшают производительность JOIN-ов и защищают от изменений в исходных данных.

![er-diagram](https://github.com/AIZharau/t1-dwh-dev-openschool/blob/main/Practice_5_GP_database_query/image/ER-model-star-t1-project.drawio.png)