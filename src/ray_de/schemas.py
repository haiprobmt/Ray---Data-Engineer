from typing import Literal
from pydantic import Field, model_validator, model_serializer
from .config import StrictModel
from .analytics import Analytics, validate_operation


class CloudProposal(StrictModel):
    operation: Literal["create_item", "update_item", "update_definition", "run_job", "deploy_to_test", "publish_environment", "tenant_action"]
    workspace_id: str
    item_id: str
    definition_path: str


class LocalArtifact(StrictModel):
    path: str = Field(min_length=1, max_length=240)
    content: str = Field(max_length=6_000_000)


class TenantReadArguments(StrictModel):
    id: str = ""
    output: str = ""


class ReadRequest(StrictModel):
    operation: Literal["get_item", "list_items", "get_notebook_job", "get_job_status", "get_item_definition", "get_lakehouse", "get_sql_database", "get_environment", "list_lakehouse_tables", "lakehouse_schema", "lakehouse_count", "lakehouse_preview", "lakehouse_profile", "lakehouse_aggregate", "lakehouse_compare", "tenant_read"]
    workspace_id: str
    item_id: str
    schema_name: str = Field(default="dbo", pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
    table_name: str = Field(default="", pattern=r"^(?:[A-Za-z_][A-Za-z0-9_]{0,127})?$")
    job_id: str = ""
    part_path: str = Field(default="", max_length=240)
    offset: int = Field(default=0, ge=0, le=6000000)
    analytics: Analytics = Field(default_factory=Analytics)
    tenant_arguments: TenantReadArguments = Field(default_factory=TenantReadArguments)

    @model_serializer(mode="wrap")
    def serialize(self, handler):
        value = handler(self)
        if self.operation != "get_item_definition":
            value.pop("part_path", None)
            value.pop("offset", None)
        if self.operation not in {"lakehouse_profile", "lakehouse_aggregate", "lakehouse_compare"}:
            value.pop("analytics", None)
        if self.operation != "tenant_read":
            value.pop("tenant_arguments", None)
        else:
            value["tenant_arguments"] = self.tenant_arguments.model_dump(exclude_defaults=True)
        return value

    @model_validator(mode="after")
    def table_required(self):
        if self.operation in {"lakehouse_profile", "lakehouse_aggregate", "lakehouse_compare"}:
            validate_operation(self.operation, self.analytics)
        elif self.analytics != Analytics():
            raise ValueError("Analytical arguments belong only to analytical reads")
        if self.tenant_arguments.model_dump(exclude_defaults=True) and self.operation != "tenant_read":
            raise ValueError("Tenant arguments belong only to a tenant read")
        if self.operation != "get_item_definition" and (self.part_path or self.offset):
            raise ValueError("Definition selectors belong only to definition reads")
        if self.operation in {"get_notebook_job", "get_job_status"}:
            from .fabric import canonical_id
            canonical_id(self.job_id)
        elif self.job_id:
            raise ValueError("A job ID belongs only to a notebook output read")
        if self.operation.startswith("lakehouse_") and not self.table_name:
            raise ValueError("A table name is required for SQL reads")
        return self


class TaskPlan(StrictModel):
    goal: str = Field(max_length=1000)
    acceptance_criteria: list[str] = Field(default_factory=list, max_length=12)
    steps: list[str] = Field(default_factory=list, max_length=16)
    current_step: str = Field(max_length=500)
    completed_steps: list[str] = Field(default_factory=list, max_length=16)
    unresolved_questions: list[str] = Field(default_factory=list, max_length=8)

    @model_validator(mode="after")
    def bounded(self):
        if any(len(s) > 1000 for s in self.acceptance_criteria + self.steps + self.completed_steps + self.unresolved_questions):
            raise ValueError("Plan entries must be concise")
        return self


class TurnResult(StrictModel):
    status: Literal[
        "working", "clarification", "approval_required", "completed", "blocked", "error"
    ]
    message: str = Field(min_length=1)
    user_summary: str | None = Field(default=None, max_length=700)
    next_step: str | None = Field(default=None, max_length=300)
    user_notes: list[str] = Field(default_factory=list, max_length=5)
    recommendation: str | None
    question: str | None
    options: list[str]
    evidence: list[str]
    skills_used: list[str]
    plan: TaskPlan | None = None
    guidance_requests: list[str] = Field(default_factory=list, max_length=3)
    cloud_actions: list[CloudProposal] = Field(default_factory=list, max_length=5)
    artifacts: list[LocalArtifact] = Field(default_factory=list, max_length=20)
    read_requests: list[ReadRequest] = Field(default_factory=list, max_length=3)
    continue_work: bool = Field(default=False, description="True after a completed source stage when more work remains. Local stages require host-verified source changes and fresh review; cloud stages require successful action receipts.")

    @model_validator(mode="after")
    def meaningful(self):
        if self.guidance_requests and (self.status != "working" or self.cloud_actions or self.artifacts or self.continue_work):
            raise ValueError("Guidance requests require a working turn without writes or artifacts")
        if any(not q.strip() or len(q) > 500 for q in self.guidance_requests):
            raise ValueError("Guidance queries must be nonempty and bounded")
        if self.read_requests and (self.status != "working" or self.cloud_actions or self.artifacts or self.continue_work):
            raise ValueError("Read requests require a working turn without writes or artifacts")
        if self.continue_work and self.status != "completed":
            raise ValueError("Continuation requires a completed source stage")
        if self.status == "clarification" and (
            not self.question or not self.recommendation
        ):
            raise ValueError("Clarification requires one question and a recommendation")
        if self.status == "completed" and not self.evidence:
            raise ValueError("Completion requires evidence")
        if self.status == "blocked" and self.question and not self.recommendation:
            raise ValueError("A blocked recovery question requires a recommendation")
        if self.status not in {"clarification", "blocked"} and self.question:
            raise ValueError("A question belongs to a clarification or blocked result")
        return self


class ReviewResult(StrictModel):
    verdict: Literal["PASS", "PASS_WITH_COMMENTS", "REWORK", "BLOCK"]
    summary: str = Field(min_length=1)
    user_summary: str | None = Field(default=None, max_length=500)
    user_notes: list[str] = Field(default_factory=list, max_length=5)
    findings: list[str]
    evidence: list[str] = Field(min_length=1)
