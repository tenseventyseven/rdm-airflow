import httpx
from pydantic import TypeAdapter, BaseModel
from typing import Type

from include.models import Dataset, DatasetUsers, Project, ProjectDatasets

RDM_API_BASE_URL = "http://atest01.hpc.wehi.edu.au:3001/api"


def fetch_data(url: str, model: Type[BaseModel]):
    response = httpx.get(url)
    response.raise_for_status()
    return TypeAdapter(model).validate_python(response.json())


def post_data(url: str, payload: dict, model: Type[BaseModel]):
    response = httpx.post(url, json=payload)
    response.raise_for_status()
    return TypeAdapter(model).validate_python(response.json())


def get_project(project_id: str) -> Project:
    url = f"{RDM_API_BASE_URL}/projects/{project_id}/users"
    return fetch_data(url, Project)


def get_dataset_users(dataset_id: str) -> DatasetUsers:
    url = f"{RDM_API_BASE_URL}/datasets/{dataset_id}/users"
    return fetch_data(url, DatasetUsers)


def get_project_datasets(project_id: str) -> ProjectDatasets:
    url = f"{RDM_API_BASE_URL}/projects/{project_id}/datasets"
    return fetch_data(url, ProjectDatasets)


def make_dataset(project_id: str, dataset_id: str) -> Dataset:
    url = f"{RDM_API_BASE_URL}/datasets"
    payload = {"datasetId": dataset_id, "projectId": project_id}
    return post_data(url, payload, Dataset)
