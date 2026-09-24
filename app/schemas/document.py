from pydantic import BaseModel, Field, ConfigDict

class DocumentCreate(BaseModel):
    user_id : str = Field(min_length=1)
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1)
    client_doc_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
    )

class DocumentUpdate(BaseModel):
    content: str = Field(min_length=1)


class DocumentResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    document_id: str
    user_id: str
    title: str
    status: str
    content_version: int
    content_hash: str
    summary: str | None = None
    tags: list[str] = []
    failed_stage: str | None = None