from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
from io import BytesIO
import base64

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return redirect(request.url)

    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)

    yml_data = parse_yaml_file(file)
    

    # Render template with chart
    return render_template('index.html', chart=yml_data)

def parse_yaml_file(filename):
    with open(filename, 'r') as file:
        try:
            yaml_data = yaml.safe_load(file)
            return yaml_data
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML file: {exc}")

if __name__ == '__main__':
    app.run(debug=True)
