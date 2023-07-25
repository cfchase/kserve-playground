from flask import Flask, jsonify, request
import os
import json
from prediction import predict
app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello World!"

@app.route('/status')
def status():
    return jsonify({'status': 'ok'})


@app.route('/predictions', methods=['POST'])
def create_prediction():
    data = request.data or '{}'
    body = json.loads(data)
    return jsonify(predict(body))


if __name__ == '__main__':
    port = os.environ.get('FLASK_PORT') or 8080
    port = int(port)

    app.run(port=port,host='0.0.0.0')
