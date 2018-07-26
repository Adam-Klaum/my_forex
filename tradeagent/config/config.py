from yaml import safe_load
from dotmap import DotMap
from pathlib import Path

config = None
root = Path(__file__).parent.parent.parent

file_found = 0
paths = [Path(__file__), Path.home(), Path('/etc/tradeagent')]
config_file = 'tradeagent.conf'

for path in paths:
    path = path / config_file
    if path.is_file():
        print("Loading config from: {}".format(path))
        file_found = 1
        break

if file_found:
    with open(str(path)) as f:
        config = DotMap(safe_load(f))

else:
    raise FileExistsError('{} not found'.format(config_file))
