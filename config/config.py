import json
import os


def read_json_config():
    json_path = "{}/config/config.json".format(os.getcwd())
    with open(json_path, "r") as jc:
        data = json.load(jc)
        return data


class Configure(object):
    def __init__(self):
        self.data = read_json_config()
