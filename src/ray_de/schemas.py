from typing import Literal
from pydantic import Field, model_validator
from .config import StrictModel


class CloudProposal(StrictModel):
    operation: Literal["create_item", "update_item", "update_definition", "run_job", "deploy_to_test"]
    workspace_id: str
    item_id: str
    definition_path: str


class LocalArtifact(StrictModel):
    path: str = Field(min_length=1, max_length=240)
    content: str = Field(max_length=6_000_000)


class ReadRequest(StrictModel):
    operation: Literal["get_lakehouse", "list_lakehouse_tables", "lakehouse_schema", "lakehouse_count", "lakehouse_preview"]
    workspace_id: str
    item_id: str
    schema_name: str = Field(default="dbo", pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
    table_name: str = Field(default="", pattern=r"^(?:[A-Za-z_][A-Za-z0-9_]{0,127})?$")

    @model_validator(mode="after")
    def table_required(self):
        if self.operation.startswith("lakehouse_") and not self.table_name:
            raise ValueError("A table name is required for SQL reads")
        return self


class TurnResult(StrictModel):
    status: Literal[
        "working", "clarification", "approval_required", "completed", "blocked", "error"
    ]
    message: str = Field(min_length=1)
    recommendation: str | None
    question: str | None
    options: list[str]
    evidence: list[str]
    skills_used: list[str]
    cloud_actions: list[CloudProposal] = Field(default_factory=list, max_length=5)
    artifacts: list[LocalArtifact] = Field(default_factory=list, max_length=20)
    read_requests: list[ReadRequest] = Field(default_factory=list, max_length=3)
    continue_work: bool = False

    @model_validator(mode="after")
    def meaningful(self):
        if self.read_requests and (self.status != "working" or self.cloud_actions or self.artifacts or self.continue_work):
            raise ValueError("Read requests require a working turn without writes or artifacts")
        if self.continue_work and (self.status != "completed" or not self.cloud_actions):
            raise ValueError("Continuation requires a completed source stage with cloud actions")
        if self.status == "clarification" and (
            not self.question or not self.recommendation
        ):
            raise ValueError("Clarification requires one question and a recommendation")
        if self.status == "completed" and not self.evidence:
            raise ValueError("Completion requires evidence")
        if self.status != "clarification" and self.question:
            raise ValueError("A question belongs to a clarification result")
        return self


class ReviewResult(StrictModel):
    verdict: Literal["PASS", "PASS_WITH_COMMENTS", "REWORK", "BLOCK"]
    summary: str = Field(min_length=1)
    findings: list[str]
    evidence: list[str] = Field(min_length=1)
