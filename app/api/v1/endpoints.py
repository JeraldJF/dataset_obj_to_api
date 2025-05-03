from fastapi import APIRouter, HTTPException
from typing import List
import httpx
from datetime import datetime
import uuid
from app.schemas.dataset import DatasetResponse, DatasetCreate, DatasetMetrics
from app.services.dataset_service import DatasetService

dataset_router = APIRouter()
dataset_service = DatasetService()

@dataset_router.get("/datasets/list", response_model=List[DatasetResponse])
async def list_datasets():
    try:
        async with httpx.AsyncClient() as client:
            datasets_response = await client.get(f'{dataset_service.base_url}/datasets/list')
            if datasets_response.status_code != 200:
                raise HTTPException(
                    status_code=datasets_response.status_code,
                    detail="Failed to fetch datasets list"
                )

            datasets = datasets_response.json().get('result', [])
            enriched_datasets = []
            time_intervals = dataset_service.get_time_intervals()

            for dataset in datasets:
                dataset_id = dataset.get('dataset_id')
                if not dataset_id:
                    continue

                # Get today's metrics
                today_processed = await dataset_service.get_events_count(
                    dataset_id,
                    time_intervals['today'][0],
                    time_intervals['today'][1]
                )
                today_failed = await dataset_service.get_failed_events_count(
                    dataset_id,
                    time_intervals['today'][0],
                    time_intervals['today'][1]
                )
                today_total = today_processed + today_failed

                # Get yesterday's metrics
                yesterday_processed = await dataset_service.get_events_count(
                    dataset_id,
                    time_intervals['yesterday'][0],
                    time_intervals['yesterday'][1]
                )
                yesterday_failed = await dataset_service.get_failed_events_count(
                    dataset_id,
                    time_intervals['yesterday'][0],
                    time_intervals['yesterday'][1]
                )
                yesterday_total = yesterday_processed + yesterday_failed

                # Get dataset health
                health_status = await dataset_service.get_dataset_health(dataset_id)
                last_synced_time = await dataset_service.get_last_synced_time(dataset_id)

                enriched_datasets.append(DatasetResponse(
                    dataset=dataset.get('name', ''),
                    status=health_status.lower(),
                    last_synced_time=last_synced_time,
                    metrics=DatasetMetrics(
                        received=today_total,
                        success=today_processed,
                        failed=today_failed,
                        yesterday={
                            "received": yesterday_total,
                            "success": yesterday_processed,
                            "failed": yesterday_failed
                        }
                    )
                ))

            return enriched_datasets

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to connect to metrics API: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@dataset_router.post("/datasets/create")
async def create_dataset(dataset: DatasetCreate):
    try:
        context = dataset.context
        sample_event = dataset.sample_event or {}
        dataset_name = context.dataset_name
        data_location = context.data_location

        # Step 1: Get schema
        schema_payload = {
            "id": "api.datasets.dataschema",
            "ver": "1.0",
            "ts": datetime.utcnow().isoformat(),
            "params": {
                "msgid": str(uuid.uuid4())
            },
            "request": {
                "data": [sample_event] if sample_event else [],
                "config": {
                    "dataset": dataset_name
                }
            }
        }

        async with httpx.AsyncClient() as client:
            schema_response = await client.post(
                f'{dataset_service.base_url}/datasets/dataschema',
                json=schema_payload
            )
            
            if schema_response.status_code != 200:
                raise HTTPException(
                    status_code=schema_response.status_code,
                    detail="Failed to get schema from API"
                )

            schema_result = schema_response.json().get('result', {}).get('schema', {})
            
            if not schema_result:
                schema_result = {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "properties": {},
                    "additionalProperties": True
                }

            formatted_dataset_id = dataset_service.format_dataset_id(dataset_name)
            dataset_type = dataset_service.validate_type(context.dataset_purpose)
            
            transformations = []
            for field in context.pii_fields:
                if isinstance(field, dict) and 'field' in field:
                    transformations.append({
                        "field_key": field['field'],
                        "transformation_function": {
                            "type": "mask",
                            "expr": field.get('field', ''),
                            "category": "pii"
                        },
                        "mode": "Strict"
                    })

            for field in context.transformation_fields:
                if isinstance(field, dict) and 'field' in field:
                    transformations.append({
                        "field_key": field['field'],
                        "transformation_function": {
                            "type": "transform",
                            "expr": field.get('expr', ''),
                            "category": "transformation"
                        },
                        "mode": "Strict"
                    })

            # Create dataset payload
            dataset_payload = {
                "id": "api.datasets.create",
                "ver": "1.0",
                "ts": datetime.utcnow().isoformat(),
                "params": {
                    "msgid": str(uuid.uuid4())
                },
                "request": {
                    "dataset_id": formatted_dataset_id,
                    "type": dataset_type,
                    "name": dataset_name,
                    "validation_config": {
                        "validate": True,
                        "mode": "Strict"
                    },
                    "extraction_config": {
                        "is_batch_event": True,
                        "extraction_key": "events",
                        "dedup_config": {
                            "drop_duplicates": True,
                            "dedup_key": context.dedup_key
                        }
                    },
                    "dedup_config": {
                        "drop_duplicates": True,
                        "dedup_key": context.dedup_key
                    },
                    "data_schema": schema_result,
                    "dataset_config": {
                        "indexing_config": {
                            "olap_store_enabled": context.storage_option.lower() == 'druid' if context.storage_option else False,
                            "lakehouse_enabled": context.storage_option.lower() == 'lakehouse' if context.storage_option else False,
                            "cache_enabled": False
                        },
                        "keys_config": {
                            "timestamp_key": context.timestamp_key
                        }
                    },
                    "transformations_config": transformations,
                    "connectors_config": dataset_service.get_connector_config(data_location, dataset_name)
                }
            }

            create_response = await client.post(
                f'{dataset_service.base_url}/datasets/create',
                json=dataset_payload
            )

            return {
                'schema_response': schema_response.json(),
                'create_response': create_response.json()
            }

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to connect to API: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )