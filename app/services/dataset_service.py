from datetime import datetime, timedelta
import httpx
import uuid
import re
from typing import List, Dict, Any, Tuple
from app.core.config import get_settings

settings = get_settings()

class DatasetService:
    def __init__(self):
        self.base_url = settings.BACKEND_URL

    async def get_failed_events_count(self, dataset_id: str, start_time: datetime, end_time: datetime) -> int:
        payload = {
            "query": {
                "type": "api",
                "id": "failedEventsCountPerDataset",
                "url": "/prom/api/v1/query",
                "method": "GET",
                "params": {
                    "query": (
                        f"sum(sum_over_time(flink_taskmanager_job_task_operator_PipelinePreprocessorJob_{dataset_id}_dedup_failed_count[{int(end_time.timestamp() - start_time.timestamp())}s])) + "
                        f"sum(sum_over_time(flink_taskmanager_job_task_operator_PipelinePreprocessorJob_{dataset_id}_validator_failed_count[{int(end_time.timestamp() - start_time.timestamp())}s])) + "
                        f"sum(sum_over_time(flink_taskmanager_job_task_operator_ExtractorJob_{dataset_id}_extractor_failed_count[{int(end_time.timestamp() - start_time.timestamp())}s])) + "
                        f"sum(sum_over_time(flink_taskmanager_job_task_operator_ExtractorJob_{dataset_id}_extractor_duplicate_count[{int(end_time.timestamp() - start_time.timestamp())}s])) + "
                        f"sum(sum_over_time(flink_taskmanager_job_task_operator_TransformerJob_{dataset_id}_transform_failed_count[{int(end_time.timestamp() - start_time.timestamp())}s])) + "
                        f"sum(sum_over_time(flink_taskmanager_job_task_operator_DruidRouterJob_{dataset_id}_failed_event_count[{int(end_time.timestamp() - start_time.timestamp())}s]))"
                    ),
                    "time": int(end_time.timestamp())
                },
                "time": int(end_time.timestamp()),
                "dataset": dataset_id,
                "master": False,
                "metadata": {}
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://localhost:3005/v2/data/metrics?id=failedEventsCountPerDataset",
                json=payload
            )
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success' and isinstance(result.get('data', {}).get('result'), list):
                    # Extract value from the first result if available
                    results = result['data']['result']
                    if results and len(results) > 0 and 'value' in results[0]:
                        try:
                            return int(float(results[0]['value'][1]))
                        except (IndexError, ValueError):
                            pass
                return 0
            return 0

    async def get_events_count(self, dataset_id: str, start_time: datetime, end_time: datetime) -> int:
        payload = self._build_events_count_payload(dataset_id, start_time, end_time)
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://localhost:3005/v2/data/metrics?id=totalProcessedEventsCount",
                json=payload
            )
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success' and isinstance(result.get('data', {}).get('result'), list):
                    results = result['data']['result']
                    if results and len(results) > 0 and 'value' in results[0]:
                        try:
                            return int(float(results[0]['value'][1]))
                        except (IndexError, ValueError):
                            pass
                return 0
            return 0

    async def get_last_synced_time(self, dataset_id: str) -> int:
        payload = self._build_last_synced_time_payload(dataset_id)
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://localhost:3005/v2/data/metrics?id=lastSyncedTime",
                json=payload
            )
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success' and isinstance(result.get('data', {}).get('result'), list):
                    results = result['data']['result']
                    if results and len(results) > 0 and 'value' in results[0]:
                        try:
                            return int(float(results[0]['value'][1]))
                        except (IndexError, ValueError):
                            pass
                return 0
            return 0

    async def get_dataset_health(self, dataset_id: str) -> str:
        payload = {
            "id": "api.datasets.health",
            "ver": "v2",
            "ts": datetime.utcnow().isoformat(),
            "params": {"msgid": str(uuid.uuid4())},
            "request": {
                "dataset_id": dataset_id,
                "categories": ["infra", "processing"]
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://localhost:3005/v2/datasets/health",
                json=payload
            )
            if response.status_code == 200:
                result = response.json()
                print(result)
                if isinstance(result, dict):
                    return result.get('result', {}).get('status', 'Unknown')
                return str(result) if result else 'Unknown'
            return 'Unknown'

    def _build_events_count_payload(self, dataset_id: str, start_time: datetime, end_time: datetime) -> Dict:
        return {
            "query": {
                "id": "totalProcessedEventsCount",
                "type": "api",
                "url": "/config/v2/data/metrics",
                "method": "POST",
                "body": {
                    "context": {"dataSource": "system-events"},
                    "query": {
                        "queryType": "timeseries",
                        "dataSource": "system-events",
                        "intervals": f"{start_time.strftime('%Y-%m-%d')}T{start_time.strftime('%H:%M:%S')}/{end_time.strftime('%Y-%m-%d')}T{end_time.strftime('%H:%M:%S')}",
                        "granularity": {
                            "type": "all",
                            "timeZone": "Asia/Kolkata"
                        },
                        "filter": {
                            "type": "and",
                            "fields": [
                                {"type": "selector", "dimension": "ctx_module", "value": "processing"},
                                {"type": "selector", "dimension": "ctx_dataset", "value": dataset_id},
                                {"type": "selector", "dimension": "ctx_pdata_pid", "value": "router"},
                                {"type": "selector", "dimension": "error_code", "value": None}
                            ]
                        },
                        "aggregations": [
                            {"type": "longSum", "name": "count", "fieldName": "count"}
                        ]
                    }
                }
            }
        }

    def _build_last_synced_time_payload(self, dataset_id: str) -> Dict:
        return {
            "query": {
                "id": "lastSyncedTime",
                "type": "api",
                "url": "/config/v2/data/metrics",
                "method": "POST",
                "body": {
                    "context": {"dataSource": "system-events"},
                    "query": {
                        "queryType": "groupBy",
                        "dataSource": "system-events",
                        "intervals": "2000-01-01/" + (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+05:30"),
                        "granularity": {
                            "type": "all",
                            "timeZone": "Asia/Kolkata"
                        },
                        "filter": {
                            "type": "and",
                            "fields": [
                                {"type": "selector", "dimension": "ctx_module", "value": "processing"},
                                {"type": "selector", "dimension": "ctx_dataset", "value": dataset_id},
                                {"type": "selector", "dimension": "ctx_pdata_pid", "value": "router"}
                            ]
                        },
                        "aggregations": [
                            {"type": "longMax", "name": "last_synced_time", "fieldName": "__time"}
                        ]
                    }
                }
            }
        }

    @staticmethod
    def get_time_intervals() -> Dict[str, Tuple[datetime, datetime]]:
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = now
        yesterday_start = (today_start - timedelta(days=1))
        yesterday_end = yesterday_start.replace(hour=23, minute=59, second=59)
        return {
            'today': (today_start, today_end),
            'yesterday': (yesterday_start, yesterday_end)
        }

    @staticmethod
    def format_dataset_id(dataset_name: str) -> str:
        formatted_id = dataset_name.lower()
        formatted_id = re.sub(r'[^a-z0-9.-]', '-', formatted_id)
        return formatted_id

    @staticmethod
    def validate_type(dataset_type: str) -> str:
        valid_types = ['event', 'transaction', 'master']
        return dataset_type.lower() if dataset_type.lower() in valid_types else 'event'

    @staticmethod
    def get_connector_config(data_location: str, dataset_name: str) -> List[Dict]:
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