from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging
import pendulum
import vertica_python

dag = DAG(
     dag_id="1_data_import",
     schedule_interval="@daily",
     start_date=pendulum.parse("2022-10-01"),
     end_date=pendulum.parse("2022-10-31"),
     catchup=True,
     tags=["de0-project-final"]
     )

log = logging.getLogger(__name__)

postgres_conn_id = "postgresql_de"
vertica_schema = "STV2025042911__STAGING"

conn_info = {"host": Variable.get("VERTICA_DB_HOST"),
             "port": Variable.get("VERTICA_DB_PORT"),
             "user": Variable.get("VERTICA_DB_USER"),
             "password": Variable.get("VERTICA_DB_PASSWORD"),
             "database": Variable.get("VERTICA_DB_DB"),
             "autocommit": True}

log.info(f"{datetime.utcnow()}: START")


def upload_to_vertica(table, date_column, conn_info):
    conn = vertica_python.connect(**conn_info)
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    try:
        # Getting the last uploaded date.
        with conn.cursor() as cur:
            cur.execute(f"""
                        SELECT NVL(MAX({date_column}), '1900-01-01'::TIMESTAMP)
                        FROM {vertica_schema}.{table};
                        """)
            date = cur.fetchone()[0]

        # Getting the PostgreSQL data.
        log.info("Fetching data from the source")
        df = postgres_hook.get_pandas_df(f"""
                                        SELECT * FROM {table}
                                        WHERE {date_column} > '{date}';
                                        """)
        columns = ", ".join(list(df.columns))
        log.info(f"{df.shape[0]} events to upload")

        # Uploading data to Vertica.
        log.info("Uploading data to Vertica")
        with conn.cursor() as cur:
            cur.copy(f"""
                    COPY {vertica_schema}.{table} ({columns})
                    FROM stdin DELIMITER ','
                    REJECTED DATA AS TABLE {vertica_schema}.{table}_rej;
                    """,
                     df.to_csv(header=None, index=False))

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        log.info(f"{datetime.utcnow()}: FINISH")


load_currencies = PythonOperator(
            task_id="import_currencies",
            python_callable=upload_to_vertica,
            op_kwargs={"table": "currencies",
                       "date_column": "date_update",
                       "conn_info": conn_info
                       },
            dag=dag
        )

load_transactions = PythonOperator(
            task_id="import_transactions",
            python_callable=upload_to_vertica,
            op_kwargs={"table": "transactions",
                       "date_column": "transaction_dt",
                       "conn_info": conn_info
                       },
            dag=dag
        )

load_currencies >> load_transactions
