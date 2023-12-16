import os
from flask import Flask, request
from sales_api import import_and_save_data

app = Flask(__name__)

@app.route("/", methods=['POST'])
def process_request():
    data = request.get_json()
    raw_dir = data.get('raw_dir')
    stg_dir = data.get('stg_dir')
    import_and_save_data(raw_dir, stg_dir)
    if not raw_dir or not stg_dir:
        return '', 500
    else:
        return '', 201


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082)