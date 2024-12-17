from pydantic import BaseModel, Field


class TransferRequestModel(BaseModel):
    projectId: str = Field(min_length=1)
    sessionId: str = Field(min_length=1)
    submitter: str = Field(min_length=1)
    recipient: str = Field(min_length=1)
    srcFolder: str = Field(min_length=1)
    destFolder: str = Field(min_length=1)
    notes: str | None = None
