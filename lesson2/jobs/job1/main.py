import os
from flask import Flask, request
from sales_api import import_data_from_pages, save_to_disk

app = Flask(__name__)

SECRET_TOKEN = os.getenv('AUTH_TOKEN')

@app.route('/', methods=['POST'])
def hello_world():
    request_data = request.get_json()
    date = request_data['date']
    raw_dir = request_data['raw_dir']
    data = import_data_from_pages(date)
    if not save_to_disk(data, raw_dir, date):
        return '', 500
    else:
        return '', 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081)