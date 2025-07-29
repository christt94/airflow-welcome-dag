from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
import requests
import logging

def print_welcome():
    logging.info('Welcome to Airflow!')

def print_date():
    logging.info('Today is {}'.format(datetime.today().date()))

def print_random_quote():
    try:
        response = requests.get('https://api.quotable.io/random', timeout=5)
        response.raise_for_status()
        quote = response.json().get('content', 'No quote found')
        logging.info('Quote of the day: "{}"'.format(quote))
    except Exception as e:
        logging.error(f"Failed to fetch quote: {e}")

default_args = {
    'start_date': pendulum.now().subtract(days=1)
}

with DAG(
    dag_id='welcome_dag',
    default_args=default_args,
    schedule='0 23 * * *',  # ✅ Use `schedule`
    catchup=False,
    tags=['example']
) as dag:

    welcome_task = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome
    )

    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )

    quote_task = PythonOperator(
        task_id='print_random_quote',
        python_callable=print_random_quote
    )

    welcome_task >> date_task >> quote_task
