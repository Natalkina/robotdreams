import os
import json
import fastavro
import shutil

SECRET_TOKEN = os.getenv('AUTH_TOKEN')


def import_and_save_data(raw_dir, stg_dir):
    avro_schema = {
        "type": "record",
        "name": "SaleRecord",
        "fields": [
            {"name": "client", "type": "string"},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"}
        ]
    }
    raw_file_path = os.path.join(raw_dir, 'sales_2022-08-09.json')

    try:
        with open(raw_file_path, 'r') as json_file:
            all_data = json.load(json_file)
    except FileNotFoundError:
        print(f"JSON file not found: {raw_file_path}. Stopping import.")
        return
    except json.JSONDecodeError:
        print(f"Invalid JSON in file: {raw_file_path}. Stopping import.")
        return

    if os.path.exists(stg_dir):
        shutil.rmtree(stg_dir)
    os.makedirs(stg_dir)

    avro_filename = os.path.join(stg_dir, 'sales_2022-08-09.avro')
    with open(avro_filename, 'wb') as avro_file:
        fastavro.writer(avro_file, avro_schema, all_data)
        avro_file.close()

