from pydantic import BaseModel, EmailStr, Field


class RequestFormModel(BaseModel):
    instrumentId: str = Field(min_length=1)
    projectId: str = Field(min_length=1)
    submitter: str = Field(pattern=r"^[a-z]+\.[a-z]+$", min_length=1)
    recipient: str = Field(pattern=r"^[a-z]+\.[a-z]+$", min_length=1)
    srcFolder: str = Field(min_length=1)
    sessionId: str = Field(min_length=1)
    notes: str | None = None


class Project(BaseModel):
    id: int
    projectId: str
    userIds: list[str]


class ProjectDatasets(BaseModel):
    id: int
    projectId: str
    datasetIds: list[str]


class Dataset(BaseModel):
    id: int
    datasetId: str
    projectId: str


class DatasetUsers(BaseModel):
    id: int
    datasetId: str
    userIds: list[str]


class UserModel(BaseModel):
    name: str
    email: EmailStr
