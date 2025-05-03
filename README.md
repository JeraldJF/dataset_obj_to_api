# Dataset API

A FastAPI-based service for managing datasets and their metrics.

## Prerequisites

- Python 3.12 or higher
- pip (Python package installer)
- Virtual environment tool (venv)

## Setup and Installation

1. Create and activate a Python virtual environment:
```bash
# Create virtual environment
python3 -m venv venv

# Activate on Unix/macOS
source venv/bin/activate

# Activate on Windows
# venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Environment Configuration:
Create a `.env` file in the root directory with:
```
PROJECT_NAME="Dataset API"
BACKEND_URL=http://localhost:3000
```

4. Run the application:
```bash
uvicorn app.main:app --host 0.0.0.0 --port 3000 --reload
```

## API Documentation

Once the server is running, access the API documentation at:
- Swagger UI: http://localhost:3000/docs
- ReDoc: http://localhost:3000/redoc

## API Endpoints

### GET /datasets/list
Lists all datasets with their metrics and health status.

**Response Example:**
```json
[
  {
    "dataset": "example-dataset",
    "status": "active",
    "last_synced_time": 1682956800,
    "metrics": {
      "received": 1000,
      "success": 950,
      "failed": 50,
      "yesterday": {
        "received": 900,
        "success": 880,
        "failed": 20
      }
    }
  }
]
```

### POST /datasets/create
Creates a new dataset with the specified configuration.

**Request Body Example:**
```json
{
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
```

## Development

- The application uses FastAPI for better performance and async support
- Environment variables are managed through python-dotenv
- HTTP requests are handled using httpx for async operations
- Built-in API documentation with OpenAPI/Swagger support
- Automatic data validation using Pydantic models

## Error Handling

The API includes comprehensive error handling:
- HTTP 400: Bad Request (Invalid input data)
- HTTP 404: Not Found
- HTTP 500: Internal Server Error
- HTTP 503: Service Unavailable (Backend service unreachable)

## Monitoring

The API provides metrics for:
- Total events received
- Successful events
- Failed events
- Historical comparisons (today vs. yesterday)
- Dataset health status