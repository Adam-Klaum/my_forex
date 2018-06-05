import yaml
from pathlib import Path


class OAConf(object):

    def __init__(self):

        home = str(Path.home())

        with open(home + "/v20.conf", 'r') as stream:
            try:
                conf_dict = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        self.c_list = []

        for key, value in conf_dict.items():
            setattr(self, key, value)
            self.c_list.append(key)

