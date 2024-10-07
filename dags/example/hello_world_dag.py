import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


# Define the DAG using the TaskFlow API
@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    tags=["example"],
    catchup=False,
)
def hello_world_dag():
    @task
    def print_hello():
        print("Hello, World!")

    return (
        EmptyOperator(task_id="start") >> print_hello() >> EmptyOperator(task_id="end")
    )


hello_world_dag()
