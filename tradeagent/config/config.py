from yaml import safe_load
from os import curdir
from os.path import expanduser, join, isfile, abspath
from dotmap import DotMap

config = None

file_found = 0
paths = [curdir, expanduser('~'), '/etc/tradeagent']
config_file = 'tradeagent.conf'

for path in paths:
    path = abspath(join(path, config_file))
    if isfile(path):
        print("Loading config from: {}".format(path))
        file_found = 1
        break

if file_found:
    with open(path) as f:
        config = DotMap(safe_load(f))

else:
    raise FileExistsError('{} not found'.format(config_file))
