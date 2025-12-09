# transaction-metrics
### Описание проекта
#### Сбор данных для аналитики динамики оборота компании
Задача: собрать данные для финтех-стартапа по транзакционной активности пользователей и настроить обновление таблицы с курсами валют.
- DAG `/src/1_data_import`: по расписанию берет новые данные из PostgreSQL (через PostgresHook) для таблиц currencies и transactions, определяет последнее загруженное по колонке даты в таблице Vertica, выгружает дельту в pandas DataFrame и загружает её в Vertica через COPY. Задачи: import_currencies → import_transactions.
- DAG `/src/2_datamart_update`: по тому же расписанию читает SQL-файл (`/sql/SQL_DWH.sql`) и выполняет его в Vertica для обновления витрины (задача update_datamart).
### Стек технологий
- Apache Airflow (DAG, PythonOperator, Variables)
- PostgreSQL (источник, PostgresHook)
- Vertica (хранилище, vertica_python)
- Python (pandas через PostgresHook, logging, pendulum)
### Результат
Осуществлен ежедневный ETL‑процесса по получению информации и подготовки ее для анализа.
- Создано хранилище для загрузки сырых данных.
- Реализован ежедневный пайплайн загрузки и обработки данных.
- Организована обновляемая витрина с курсом валют.
