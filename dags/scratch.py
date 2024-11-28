import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from include.models import UserModel


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def scratch():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    @task
    def get_user() -> UserModel:
        user = UserModel(name="Foo", email="foo@bar.com")

        return user

    @task
    def print_user(user: UserModel) -> UserModel:
        print(user)

        return user

    user = get_user()

    start >> user >> print_user(user) >> end


scratch()
