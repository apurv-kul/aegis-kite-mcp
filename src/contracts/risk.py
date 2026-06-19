from typing import List
from pydantic import BaseModel, Field, PositiveFloat, field_validator

class RiskAssessment(BaseModel):
    is_approved: bool = Field(...)
    approved_lots: int = Field(
        ..., ge=0,
        description=(
            "Number of lots approved for execution. "
            "May be less than requested when confidence < 75 (50% size reduction) "
            "or when margin utilisation would exceed the 70% guardrail. "
            "Zero only when is_approved is False. "
            "ExecutionNode multiplies this by lot_size to derive final quantity."
        ),
    )
    max_calculated_loss: PositiveFloat = Field(...)
    margin_required: PositiveFloat = Field(...)
    tripped_risk_rules: List[str] = Field(default_factory=list)
    risk_engine_version: str = Field(..., min_length=5, max_length=32)

    model_config = {"frozen": True, "extra": "forbid"}

    @field_validator("tripped_risk_rules")
    @classmethod
    def ensure_rules_on_reject(cls, value, info):
        if not info.data.get("is_approved") and len(value) == 0:
            raise ValueError("Rejected risk assessments must provide at least one tripped risk rule")
        return value

    @field_validator("approved_lots")
    @classmethod
    def lots_zero_on_reject(cls, value, info):
        if not info.data.get("is_approved") and value > 0:
            raise ValueError("approved_lots must be 0 when is_approved is False")
        if info.data.get("is_approved") and value == 0:
            raise ValueError("approved_lots must be >= 1 when is_approved is True")
        return value