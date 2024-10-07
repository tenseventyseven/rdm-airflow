from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

import pendulum
import httpx
from pydantic import TypeAdapter
import os
import uuid
import subprocess

from include.models import DatasetUsers, ProjectDatasets, RequestFormModel, Project, Dataset

RDM_API_BASE_URL = "http://atest01.hpc.wehi.edu.au:3001/api"
SRC_BASE_PATH = "/mnt/shared"
DEST_BASE_PATH = "/vast/projects/ResearchDataManagement"

with DAG(
    dag_id="data_ingest",
    default_args={
        "owner": "airflow",
        # "email": ["alerts@example.com"],
        # "email_on_failure": True,
        # "email_on_retry": False,
        # "retries": 0,
        # "retry_delay": pendulum.duration(minutes=5),
    },
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    @task
    def foo(params) -> dict[str, str]:
        return dict(RequestFormModel(**params))

    @task
    def validate_request(params) -> None:
        # Is the shape of the request correct?
        # Does the submitter and recipient have permissions to the project?
        # Does the source folder exist and is non-empty?

        # Parse and validate params using the Pydantic model
        request = RequestFormModel(**params)
        submitter = request.submitter
        recipient = request.recipient
        projectId = request.projectId
        srcFolder = request.srcFolder

        # Validate submitter for project
        URL = f"{RDM_API_BASE_URL}/projects/{projectId}/users"
        response = httpx.get(URL)
        response.raise_for_status()

        project_adapter = TypeAdapter(Project)
        project = project_adapter.validate_python(response.json())

        if submitter not in project.userIds:
            raise AirflowFailException(
                f"Submitter {submitter} missing permissions to {projectId}"
            )
        else:
            print(f"Submitter {submitter} has correct permissions")

        if recipient not in project.userIds:
            raise AirflowFailException(
                f"Recipient {recipient} missing permissions to {projectId}"
            )
        else:
            print(f"Recipient {recipient} has correct permissions")

        # Check if the directory exists
        dir_path = f"/mnt/shared/{srcFolder}"
        if not os.path.isdir(dir_path):
            raise AirflowFailException(f"{dir_path} does not exist or is not a directory.")

        # Check if the directory is readable
        if os.access(dir_path, os.R_OK):
            print(f"{dir_path} exists and is readable.")
        else:
            raise AirflowFailException(f"{dir_path} exists but is not readable.")

    @task
    def create_dataset(params) -> str:
        project_id = params["projectId"]

        # TODO: get this value from data portal API..
        dataset_id = str(uuid.uuid4())

        payload = {"datasetId": dataset_id, "projectId": project_id}

        print(payload)

        URL = f"{RDM_API_BASE_URL}/datasets"
        response = httpx.post(URL, json=payload)
        response.raise_for_status()

        dataset_adapter = TypeAdapter(Dataset)
        dataset = dataset_adapter.validate_python(response.json())

        if not dataset.datasetId == dataset_id:
            raise AirflowFailException(f"Dataset creation failed")

        return dataset.datasetId

    @task
    def create_dataset_folder(dataset_id: str) -> str:
        dataset_folder = f"{DEST_BASE_PATH}/{dataset_id}"

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
        return f"rsync -avzP --partial --ignore-existing {src_path} {dest_path}"

    @task
    def apply_dataset_access_controls(dataset_id: str, dataset_folder: str) -> None:
        # Get user IDs for dataset
        URL = f"{RDM_API_BASE_URL}/datasets/{dataset_id}/users"
        response = httpx.get(URL)
        response.raise_for_status()

        dataset_users_adapter = TypeAdapter(DatasetUsers)
        dataset_users = dataset_users_adapter.validate_python(response.json())

        for user in dataset_users.userIds:
            # TODO: skip when userId == process owner

            # Construct the setfacl command
            setfacl_command = f'setfacl -Rm u:{user}:rwX,d:u:{user}:rwX {dataset_folder}'
            
            try:
                # Use subprocess to execute the setfacl command
                result = subprocess.run(setfacl_command, shell=True, check=True, text=True, capture_output=True)
                
                print(f"Permissions applied for {user}")
            except subprocess.CalledProcessError as e:
                # Handle the case where the command fails
                print(f"Failed to apply permissions for {user}: {e.stderr}")
                raise  # Reraise

    @task
    def create_project_links(params) -> None:
        project_id = params["projectId"]

        # Get user IDs for dataset
        URL = f"{RDM_API_BASE_URL}/projects/{project_id}/datasets"
        response = httpx.get(URL)
        response.raise_for_status()

        project_datasets_adapter = TypeAdapter(ProjectDatasets)
        project_datasets = project_datasets_adapter.validate_python(response.json())

        for dataset in project_datasets.datasetIds:
            source_path = os.path.join(DEST_BASE_PATH, dataset)
            target_path = os.path.join(DEST_BASE_PATH, project_id, dataset)
        
            # Print out what you're about to do for debugging
            print(f"Creating soft link from {source_path} to {target_path}")
            
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

    # Setup tasks
    dataset_id = create_dataset()
    dataset_folder = create_dataset_folder(dataset_id)

    # Define workflow
    (
        start
        >> validate_request()
        >> dataset_id
        >> dataset_folder
        >> [write_request_form_metadata(dataset_folder), perform_dataset_copy(dataset_folder)]
        >> apply_dataset_access_controls(dataset_id, dataset_folder)
        >> create_project_links()
        >> end
    )
