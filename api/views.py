from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import requests
import uuid
from datetime import datetime
import re

class DatasetView(APIView):
    def format_dataset_id(self, dataset_name):
        # Convert to lowercase and replace invalid chars with -
        formatted_id = dataset_name.lower()
        # Replace any character that's not a-z, 0-9, period, or hyphen with hyphen
        formatted_id = re.sub(r'[^a-z0-9.-]', '-', formatted_id)
        return formatted_id

    def validate_type(self, dataset_type):
        valid_types = ['event', 'transaction', 'master']
        return dataset_type.lower() if dataset_type.lower() in valid_types else 'event'

    def get_connector_config(self, data_location, dataset_name):
        connector_id = f"{data_location}-connector-{uuid.uuid4().hex[:8]}"
        
        if data_location.lower() == 'kafka':
            return [{
                "id": data_location.lower(),
                "connector_id": connector_id,
                "connector_config": {
                    "brokers": ["localhost:9092"],
                    "topic": f"{dataset_name.lower()}-events"
                },
                "operations_config": {},
                "version": "1.0"
            }]
        return []

    def post(self, request):
        try:
            data = request.data
            context = data.get('context', {})
            sample_event = context.get('sample_event', {})
            dataset_name = context.get('dataset_name')
            data_location = context.get('data_location')
            
            # Step 1: Get schema from Node.js API
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

            schema_response = requests.post(
                'http://localhost:3000/v2/datasets/dataschema',
                json=schema_payload
            )
            
            if schema_response.status_code != 200:
                return Response(
                    {"error": "Failed to get schema from Node.js API"},
                    status=schema_response.status_code
                )

            # Get schema result from the nested structure
            schema_result = schema_response.json().get('result', {}).get('schema', {})
            
            # Ensure the schema has all required properties
            if not schema_result:
                schema_result = {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "properties": {},
                    "additionalProperties": True
                }

            # Format dataset_id from dataset_name
            formatted_dataset_id = self.format_dataset_id(dataset_name)

            # Get and validate dataset type
            dataset_type = self.validate_type(context.get('dataset_purpose', 'event'))

            # Process transformation fields
            transformations = []
            
            # Handle PII fields
            for field in context.get('pii_fields', []):
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

            # Handle transformation fields if they exist
            for field in context.get('transformation_fields', []):
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

            # Step 2: Create dataset using the generated schema
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
                            "dedup_key": context.get('dedup_key')
                        }
                    },
                    "dedup_config": {
                        "drop_duplicates": True,
                        "dedup_key": context.get('dedup_key')
                    },
                    "data_schema": schema_result,
                    "dataset_config": {
                        "indexing_config": {
                            "olap_store_enabled": context.get('storage_option', '').lower() == 'druid',
                            "lakehouse_enabled": context.get('storage_option', '').lower() == 'lakehouse',
                            "cache_enabled": False
                        },
                        "keys_config": {
                            "timestamp_key": context.get('timestamp_key')
                        }
                    },
                    "transformations_config": transformations,
                    "connectors_config": self.get_connector_config(data_location, dataset_name)
                }
            }

            create_response = requests.post(
                'http://localhost:3000/v2/datasets/create',
                json=dataset_payload
            )

            return Response({
                'schema_response': schema_response.json(),
                'create_response': create_response.json()
            }, status=create_response.status_code)

        except requests.RequestException as e:
            return Response(
                {"error": f"Failed to connect to Node.js API: {str(e)}"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE
            )
        except Exception as e:
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
