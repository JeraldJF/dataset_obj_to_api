from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime

class DatasetMetrics(BaseModel):
    received: int = 0
    success: int = 0
    failed: int = 0
    yesterday: Dict[str, int] = Field(
        default_factory=lambda: {
            "received": 0,
            "success": 0,
            "failed": 0
        }
    )

class DatasetResponse(BaseModel):
    dataset: str
    status: str
    last_synced_time: datetime | None = None
    metrics: DatasetMetrics

class TransformationField(BaseModel):
    field: str
    expr: Optional[str] = None

class PIIField(BaseModel):
    field: str
    type: str = "mask"

class ConnectorConfig(BaseModel):
    id: str
    connector_id: str
    connector_config: Dict[str, Any]
    operations_config: Dict[str, Any] = Field(default_factory=dict)
    version: str = "1.0"

class DatasetContext(BaseModel):
    dataset_name: str
    data_location: str
    dataset_purpose: Optional[str] = "event"
    dedup_key: Optional[str] = None
    timestamp_key: Optional[str] = None
    storage_option: Optional[str] = None
    pii_fields: List[Dict[str, str]] = Field(default_factory=list)
    transformation_fields: List[Dict[str, str]] = Field(default_factory=list)

class DatasetCreate(BaseModel):
    context: DatasetContext
    sample_event: Optional[Dict[str, Any]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "context": {
                    "dataset_name": "test-dataset",
                    "data_location": "kafka",
                    "dataset_purpose": "event",
                    "dedup_key": "id",
                    "timestamp_key": "timestamp",
                    "storage_option": "druid",
                    "pii_fields": [{"field": "email"}],
                    "transformation_fields": [{"field": "status", "expr": "UPPER(status)"}]
                },
                "sample_event": {
                    "id": "123",
                    "timestamp": "2025-05-03T10:00:00Z",
                    "email": "test@example.com",
                    "status": "active"
                }
            }
        }