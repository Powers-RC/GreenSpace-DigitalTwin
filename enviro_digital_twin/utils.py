import json
from base64 import b64encode

def compose_firehose_resposne(record_id: int, status: str, data: dict) -> dict:
    data_bytes = json.dumps(data).encode('utf-8')
    return {
        "recordId": record_id,
        "status": status,
        "data" : b64encode(data_bytes) 
    }
