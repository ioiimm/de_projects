from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging
import pendulum
import vertica_python

dag = DAG(
     dag_id="2_datamart_update",
     schedule_interval="@daily",
     start_date=pendulum.parse("2022-10-01"),
     end_date=pendulum.parse("2022-10-31"),
     catchup=True,
     tags=["de0-project-final"]
     )

log = logging.getLogger(__name__)

conn_info = {"host": Variable.get("VERTICA_DB_HOST"),
             "port": Variable.get("VERTICA_DB_PORT"),
             "user": Variable.get("VERTICA_DB_USER"),
             "password": Variable.get("VERTICA_DB_PASSWORD"),
             "database": Variable.get("VERTICA_DB_DB"),
             "autocommit": True}

log.info(f"{datetime.utcnow()}: START")


def update_datamart(sql_path, conn_info):
    try:
        log.info("Reading SQL file")
        with open(sql_path, "r") as f:
            sql = f.read()

        with vertica_python.connect(**conn_info) as conn:
            log.info("Updating datamart in Vertica")
            cur = conn.cursor()
            cur.execute(sql)
            cur.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        log.info(f"{datetime.utcnow()}: FINISH")


load_datamart = PythonOperator(
            task_id="update_datamart",
            python_callable=update_datamart,
            op_kwargs={"sql_path": "/sql/SQL_DWH.sql",
                       "conn_info": conn_info
                       },
            dag=dag
        )

load_datamart
