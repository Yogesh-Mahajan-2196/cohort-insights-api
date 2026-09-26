from pydantic import BaseModel, ConfigDict, Field


class DocumentCreate(BaseModel):
    user_id: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1)
    client_doc_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
    )


class DocumentUpdate(BaseModel):
    user_id: str = Field(min_length=1, max_length=100)
    content: str = Field(min_length=1)


class StageResponse(BaseModel):
    status: str
    content_version: int | None = None


class DocumentResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    document_id: str
    user_id: str
    title: str
    client_doc_ref: str | None = None

    status: str
    content_version: int
    content_hash: str

    processing: StageResponse
    enriching: StageResponse

    summary: str | None = None
    tags: list[str] = Field(default_factory=list)
    failed_stage: str | None = None
    is_stale: bool
