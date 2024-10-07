import httpx
from pydantic import TypeAdapter

from include.models import Dataset, DatasetUsers, Project, ProjectDatasets

RDM_API_BASE_URL = "http://atest01.hpc.wehi.edu.au:3001/api"


def get_project(project_id: str) -> Project:
    URL = f"{RDM_API_BASE_URL}/projects/{project_id}/users"
    response = httpx.get(URL)
    response.raise_for_status()

    project_adapter = TypeAdapter(Project)
    project = project_adapter.validate_python(response.json())
    return project


def get_dataset_users(dataset_id: str) -> DatasetUsers:
    URL = f"{RDM_API_BASE_URL}/datasets/{dataset_id}/users"
    response = httpx.get(URL)
    response.raise_for_status()

    dataset_users_adapter = TypeAdapter(DatasetUsers)
    dataset_users = dataset_users_adapter.validate_python(response.json())
    return dataset_users


def get_project_datasets(project_id: str) -> ProjectDatasets:
    URL = f"{RDM_API_BASE_URL}/projects/{project_id}/datasets"
    response = httpx.get(URL)
    response.raise_for_status()

    project_datasets_adapter = TypeAdapter(ProjectDatasets)
    project_datasets = project_datasets_adapter.validate_python(response.json())
    return project_datasets


def make_dataset(project_id: str, dataset_id: str) -> Dataset:
    URL = f"{RDM_API_BASE_URL}/datasets"
    payload = {"datasetId": dataset_id, "projectId": project_id}
    response = httpx.post(URL, json=payload)
    response.raise_for_status()

    dataset_adapter = TypeAdapter(Dataset)
    dataset = dataset_adapter.validate_python(response.json())
    return dataset
