# Лекция 5: Практическое задание 

> [!**Задание  R5.1**]
> Скопируйте в GreenPlum таблицу payments из базы t1_dwh_potok3_datasandbox в собственную базу и схему, созданные ранее. Создайте две копии таблицы payments_compressed_row и payments_compressed_columnar. Таблица payments_compressed_columnar должна иметь тип сжатия RLE, уровень сжатия 4 и колоночный тип хранения. Таблица payments_compressed_row должна иметь тип сжатия ZTSD, уровень сжатия 7 и строчный тип хранения. Используйте документацию при построении таблиц.
> **Формат ответа**: SQL-скрипты в текстовом документе и таблицы в вашей схеме в базе. Планы выполнения запросов и ваш вывод по эффективности выполненных преобразований над таблицами.


## Планы выполнения запросов и ваш вывод по эффективности выполненных преобразований над таблицами.

```SQL
explain
select * from payments_compressed_row t_1 
join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id;
```

> [!QUERY PLAN]
> Gather Motion 9:1  (slice2; segments: 9)  (cost=0.00..932.19 rows=230000 width=86)
>     ->  Hash Join  (cost=0.00..878.61 rows=25556 width=86)
          Hash Cond: (payments_compressed_row.loan_id = loans_n.loan_id) 
          ->  Seq Scan on payments_compressed_row  (cost=0.00..431.67 rows=25556 width=34)
          ->  Hash  (cost=431.37..431.37 rows=1000 width=52)
              ->  Broadcast Motion 9:9  (slice1; segments: 9)  (cost=0.00..431.37 rows=1000 width=52)
                  ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52)
> Optimizer: Pivotal Optimizer (GPORCA)

```SQL
explain
select * from payments_compressed_columnar t_1 
join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id
```

> [!QUERY PLAN]
> Gather Motion 9:1  (slice2; segments: 9)  (cost=0.00..932.19 rows=230000 width=86)
  ->  Hash Join  (cost=0.00..878.61 rows=25556 width=86)
        Hash Cond: (payments_compressed_columnar.loan_id = loans_n.loan_id)
        ->  Seq Scan on payments_compressed_columnar  (cost=0.00..431.67 rows=25556 width=34)
        ->  Hash  (cost=431.37..431.37 rows=1000 width=52)
              ->  Broadcast Motion 9:9  (slice1; segments: 9)  (cost=0.00..431.37 rows=1000 width=52)
                    ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52)
> Optimizer: Pivotal Optimizer (GPORCA)

```SQL
explain analyse
select * from payments_compressed_row t_1 
join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id;
```

> [!QUERY PLAN]
> Gather Motion 9:1  (slice2; segments: 9)  (cost=0.00..932.19 rows=230000 width=86) (actual time=27.352..83.266 rows=230000 loops=1)
>   ->  Hash Join  (cost=0.00..878.61 rows=25556 width=86) (actual time=2.002..16.024 rows=25702 loops=1)
>       Hash Cond: (payments_compressed_row.loan_id = loans_n.loan_id)
>       Extra Text: (seg5)   Hash chain length 1.0 avg, 1 max, using 1000 of 262144 buckets.
>       ->  Seq Scan on payments_compressed_row  (cost=0.00..431.67 rows=25556 width=34) (actual time=0.596..5.303 rows=25702 loops=1)
>       ->  Hash  (cost=431.37..431.37 rows=1000 width=52) (actual time=24.221..24.221 rows=1000 loops=1)
>               ->  Broadcast Motion 9:9  (slice1; segments: 9)  (cost=0.00..431.37 rows=1000 width=52) (actual time=0.037..23.676 rows=1000 loops=1)
>                     ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52) (actual time=0.020..0.040 rows=126 loops=1)
> Planning time: 21.521 ms
  (slice0)    Executor memory: 169K bytes.
  (slice1)    Executor memory: 58K bytes avg x 9 workers, 58K bytes max (seg0).
  (slice2)    Executor memory: 2588K bytes avg x 9 workers, 2602K bytes max (seg0).  Work_mem: 82K bytes max.
> Memory used:  128000kB
> Optimizer: Pivotal Optimizer (GPORCA)
> Execution time: 117.738 ms

```SQL
explain analyse
select * from payments_compressed_columnar t_1 
join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id;
```
> [!QUERY PLAN]
> Gather Motion 9:1  (slice2; segments: 9)  (cost=0.00..932.19 rows=230000 width=86) (actual time=21.947..77.221 rows=230000 loops=1)
>  ->  Hash Join  (cost=0.00..878.61 rows=25556 width=86) (actual time=20.595..32.280 rows=25702 loops=1)
>      Hash Cond: (payments_compressed_columnar.loan_id = loans_n.loan_id)
>      Extra Text: (seg5)   Hash chain length 1.0 avg, 1 max, using 1000 of 262144 buckets.
>      ->  Seq Scan on payments_compressed_columnar  (cost=0.00..431.67 rows=25556 width=34) (actual time=1.424..6.490 rows=25702 loops=1)
>      ->  Hash  (cost=431.37..431.37 rows=1000 width=52) (actual time=7.489..7.489 rows=1000 loops=1)
>          ->  Broadcast Motion 9:9  (slice1; segments: 9)  (cost=0.00..431.37 rows=1000 width=52) (actual time=0.060..6.730 rows=1000 loops=1)
>              ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52) (actual time=0.020..0.042 rows=126 loops=1)
> Planning time: 11.325 ms
>   (slice0)    Executor memory: 746K bytes.
>   (slice1)    Executor memory: 58K bytes avg x 9 workers, 58K bytes max (seg0).
>   (slice2)    Executor memory: 3299K bytes avg x 9 workers, 3303K bytes max (seg5).  Work_mem: 82K bytes max.
> Memory used:  128000kB
> Optimizer: Pivotal Optimizer (GPORCA)
> Execution time: 109.314 ms

### row-oriented vs column-oriented:
  
  # row-oriented (строчное хранение):
- Оптимизировано для операций, которые требуют доступа ко всем столбцам строки (например, SELECT *).
- Время выполнения: 117.738 мс.
- Использование памяти: 2588K bytes на сегмент.
> Подходит для OLTP-нагрузок - частых операции чтения и записи отдельных строк.

  # column-oriented (колоночное хранение):
- Оптимизировано для операций, которые требуют доступа к определённым столбцам (например, агрегации или фильтрации).
- Время выполнения: 109.314 мс.
- Использование памяти: 3299K bytes на сегмент.
> Подходит для OLAP-нагрузок - частых аналитических запросов.

### Изменим теперь ключ распредления
```SQL
alter table payments_compressed_row set with (reorganize=True) distributed by (loan_id);
alter table payments_compressed_columnar set with (reorganize=True) distributed by (loan_id);
alter table payments_task_5_test.loans_n set with (reorganize=True) distributed by (loan_id);

analyze payments_compressed_row;
analyze payments_compressed_columnar;
analyze payments_task_5_test.loans_n;
```

```SQL
explain
select * from payments_compressed_row t_1 join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id;
```

> [!QUERY PLAN]
>  Gather Motion 9:1  (slice1; segments: 9)  (cost=0.00..930.69 rows=230000 width=86)
>     ->  Hash Join  (cost=0.00..877.11 rows=25556 width=86)
>         Hash Cond: (payments_compressed_row.loan_id = loans_n.loan_id)
>         -> Seq Scan on payments_compressed_row  (cost=0.00..431.67 rows=25556 width=34)
>         ->  Hash  (cost=431.01..431.01 rows=112 width=52)
>             ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52)
> Optimizer: Pivotal Optimizer (GPORCA)

```SQL
explain
select * from payments_compressed_columnar t_1 
join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id
```
> [!QUERY PLAN]
> Gather Motion 9:1  (slice1; segments: 9)  (cost=0.00..930.69 rows=230000 width=86)
>   ->  Hash Join  (cost=0.00..877.11 rows=25556 width=86)
>       Hash Cond: (payments_compressed_columnar.loan_id = loans_n.loan_id)
>       ->  Seq Scan on payments_compressed_columnar  (cost=0.00..431.67 rows=25556 width=34)
>       ->  Hash  (cost=431.01..431.01 rows=112 width=52)
>           ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52)
> Optimizer: Pivotal Optimizer (GPORCA)

#### После изменения ключа распределения на loan_id:
 - Планы выполнения для row-oriented и column-oriented стали идентичными.
 - Удалось избежать Broadcast Motion, так как данные теперь распределены по ключу соединения (loan_id).
 - Время выполнения и использование памяти остались практически одинаковыми.

### Построим индекс по ключу соединения
```SQL
create index ind_1 on payments_compressed_row (loan_id); 
create index ind_1_1 on payments_compressed_columnar (loan_id); 
create index ind_2 on loans_n (loan_id);

explain
select * from payments_compressed_row t_1 
join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id;
```

> [!QUERY PLAN]
> Gather Motion 9:1  (slice1; segments: 9)  (cost=0.00..930.69 rows=230000 width=86)
> ->  Hash Join  (cost=0.00..877.11 rows=25556 width=86)
>     Hash Cond: (payments_compressed_row.loan_id = loans_n.loan_id)
>       ->  Seq Scan on payments_compressed_row  (cost=0.00..431.67 rows=25556 width=34)
>       ->  Hash  (cost=431.01..431.01 rows=112 width=52)
>             ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52)
> Optimizer: Pivotal Optimizer (GPORCA)

```SQL
explain
select * from payments_compressed_columnar t_1 
join payments_task_5_test.loans_n t_2 on t_1.loan_id = t_2.loan_id;
```
> [!QUERY PLAN]
> Gather Motion 9:1  (slice1; segments: 9)  (cost=0.00..930.69 rows=230000 width=86)
> ->  Hash Join  (cost=0.00..877.11 rows=25556 width=86)
>       Hash Cond: (payments_compressed_columnar.loan_id = loans_n.loan_id)
>       ->  Seq Scan on payments_compressed_columnar  (cost=0.00..431.67 rows=25556 width=34)
>       ->  Hash  (cost=431.01..431.01 rows=112 width=52)
>             ->  Seq Scan on loans_n  (cost=0.00..431.01 rows=112 width=52)
> Optimizer: Pivotal Optimizer (GPORCA)

#### После создания индексов на loan_id:
 - Планы выполнения не изменились, так как оптимизатор (GPORCA) решил использовать Seq Scan вместо Index Scan.
 - Это связано с тем, что таблица loans_n небольшая (112 строк), и Seq Scan эффективнее, чем Index Scan.

### Итоговые выводы:
1. **Строчное хранение (Row-based)**:
    - Эффективно для операций, которые требуют чтения всех столбцов (например, `SELECT *`).
    - Использует меньше памяти для полного сканирования.
    - Подходит для OLTP-систем, где часто выполняются операции чтения и записи отдельных строк.
        
2. **Колоночное хранение (Columnar)**:
    - Эффективно для операций, которые читают только определённые столбцы (например, `SELECT column`).
    - Использует больше памяти для полного сканирования, но может быть более эффективным для аналитических запросов.
    - Подходит для OLAP-систем, где часто выполняются агрегации и аналитические запросы.
        
3. **Соединение таблиц**:
    - Для операций соединения (`JOIN`) оба формата хранения показывают схожую производительность.
    - Индексы могут не улучшать производительность, если оптимизатор считает полное сканирование более эффективным.
