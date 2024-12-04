import json
import os

def load_config():
    config_file = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config