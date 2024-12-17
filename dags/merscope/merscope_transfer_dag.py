import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from merscope.include.models import TransferRequestModel


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    params={
        "projectId": "aaa",
        "sessionId": "bbb",
        "submitter": "ccc",
        "recipient": "ddd",
        "srcFolder": "eee",
        "destFolder": "fff",
        "notes": "test test test",
    },  # Matches TransferRequestModel
)
def merscope_transfer_dag():
    @task
    def validate_request(params) -> TransferRequestModel:
        # Is the shape of the request correct?
        transfer_request = TransferRequestModel(**params)

        return transfer_request

    @task
    def print_details(transfer_request: TransferRequestModel) -> TransferRequestModel:
        print("Here are the details of the transfer request:")
        print(transfer_request.model_dump_json())

        return transfer_request

    transfer_request = validate_request()

    (
        EmptyOperator(task_id="start")
        >> transfer_request
        >> print_details(transfer_request)
        >> EmptyOperator(task_id="end")
    )


merscope_transfer_dag()
