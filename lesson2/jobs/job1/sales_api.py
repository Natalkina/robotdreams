import os
import requests
import json
from typing import Dict, List, Any
from dotenv import load_dotenv
from jsonschema import validate, ValidationError

load_dotenv()
SECRET_TOKEN = os.getenv('AUTH_TOKEN')

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'

def is_valid_record(record: Dict[str, Any]) -> bool:
    record_schema = {
        "type": "object",
        "properties": {
            "client": {"type": "string"},
            "purchase_date": {"type": "string", "format": "date"},
            "product": {"type": "string"},
            "price": {"type": "number"},
        },
        "required": ["client", "purchase_date", "product", "price"],
    }

    try:
        validate(instance=record, schema=record_schema)
        return True
    except ValidationError:
        return False

def import_data_from_pages(date: str):
    page = 1
    all_data = []

    while True:
        response = requests.get(
            url=API_URL,
            params={'date': date, 'page': page},
            headers={'Authorization': SECRET_TOKEN},
        )
        if response.status_code == 200:
            page_data = response.json()
            if not page_data:
                break
            else:
                all_data.extend(page_data)
                page += 1
        else:
            break

    return all_data


def save_to_disk(json_content: List[Dict[str, Any]], raw_dir: str, date: str) -> bool:
    for record in json_content:
        if not is_valid_record(record):
            return False

    file_path = os.path.join(raw_dir, f"sales_{date}.json")

    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w') as file:
        json.dump(json_content, file)

    return True
