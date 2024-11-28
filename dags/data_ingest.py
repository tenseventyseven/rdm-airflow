from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

import pendulum
import os
import uuid
import subprocess

from include.helpers import (
    get_dataset_users,
    get_project,
    get_project_datasets,
    make_dataset,
)
from include.models import RequestFormModel

SRC_BASE_PATH = "/mnt/shared"
DEST_BASE_PATH = "/vast/projects/ResearchDataManagement"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def data_ingest():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    @task
    def validate_request(params) -> None:
        # Is the shape of the request correct?
        request = RequestFormModel(**params)
        submitter = request.submitter
        recipient = request.recipient
        project_id = request.projectId
        src_folder = request.srcFolder

        # Does the submitter and recipient have permissions to the project?
        project = get_project(project_id)
        if submitter not in project.userIds or recipient not in project.userIds:
            raise AirflowFailException(
                f"Submitter and/or recipient missing permissions to {project_id}"
            )
        else:
            print("Submitter abd recipient have correct permissions")

        # Does the source folder exist and is non-empty?
        dir_path = f"/mnt/shared/{src_folder}"
        if not os.path.isdir(dir_path):
            raise AirflowFailException(
                f"{dir_path} does not exist or is not a directory."
            )
        if os.access(dir_path, os.R_OK):
            print(f"{dir_path} exists and is readable.")
        else:
            raise AirflowFailException(f"{dir_path} exists but is not readable.")

    @task
    def create_dataset(params) -> str:
        # TODO: get this value from data portal API..
        dataset_id = str(uuid.uuid4())

        dataset = make_dataset(params["projectId"], dataset_id)
        if not dataset.datasetId == dataset_id:
            raise AirflowFailException("Dataset creation failed")

        return dataset_id

    @task
    def create_dataset_folder(dataset_id: str) -> str:
        dataset_folder = f"{DEST_BASE_PATH}/Datasets/{dataset_id}"
        if os.path.exists(dataset_folder):
            raise FileExistsError(f"The directory {dataset_folder} already exists.")
        else:
            os.makedirs(dataset_folder)
            print(f"Directory {dataset_folder} created successfully.")
            return dataset_folder

    @task
    def write_request_form_metadata(dest_folder: str, params) -> None:
        request_form_metadata = RequestFormModel(**params)
        file_path = f"{dest_folder}/request_form_metadata.json"
        with open(file_path, "w") as f:
            f.write(request_form_metadata.model_dump_json(indent=4))

    @task.bash
    def perform_dataset_copy(dest_path: str, params) -> None:
        src_path = f"{SRC_BASE_PATH}/{params["srcFolder"]}"
        return f"rsync -avhzP --progress --partial --inplace --ignore-existing {src_path} {dest_path}"

    @task
    def apply_dataset_access_controls(dataset_id: str, dataset_folder: str) -> None:
        dataset_users = get_dataset_users(dataset_id)
        for user in dataset_users.userIds:
            # Construct the setfacl command
            setfacl_command = (
                f"setfacl -Rm u:{user}:rwX,d:u:{user}:rwX {dataset_folder}"
            )

            try:
                # Use subprocess to execute the setfacl command
                subprocess.run(
                    setfacl_command,
                    shell=True,
                    check=True,
                    text=True,
                    capture_output=True,
                )
                print(f"Permissions applied for {user}")
            except subprocess.CalledProcessError as e:
                # Handle the case where the command fails
                print(f"Failed to apply permissions for {user}: {e.stderr}")
                raise  # Reraise

    @task
    def create_project_links(params) -> None:
        project_id = params["projectId"]
        project_datasets = get_project_datasets(params["projectId"])
        for dataset in project_datasets.datasetIds:
            source_path = os.path.join(DEST_BASE_PATH, "Datasets", dataset)
            target_path = os.path.join(DEST_BASE_PATH, "Projects", project_id, dataset)

            try:
                # Create directories if they don't exist
                os.makedirs(os.path.dirname(target_path), exist_ok=True)

                # Create the symlink
                os.symlink(source_path, target_path)

                print(f"Soft link created for {dataset}")
            except FileExistsError:
                print(f"Soft link for {dataset} already exists")
            except Exception as e:
                print(f"Failed to create soft link for {dataset}: {str(e)}")
                raise  # Reraise

    # Define workflow
    dataset_id = create_dataset()
    dataset_folder = create_dataset_folder(dataset_id)

    (
        start
        >> validate_request()
        >> dataset_id
        >> dataset_folder
        >> [
            write_request_form_metadata(dataset_folder),
            perform_dataset_copy(dataset_folder),
        ]
        >> apply_dataset_access_controls(dataset_id, dataset_folder)
        >> create_project_links()
        >> end
    )


data_ingest()
