from yaml import safe_load
from os import curdir
from os.path import expanduser, join, isfile, abspath


class TAConfig(object):

    #TODO better system for existence of file?

    my_config = None

    file_found = 0
    paths = [curdir, expanduser('~'), '/etc/tradeagent']

    for path in paths:
        path = abspath(join(path, 'tradeagent.conf'))
        if isfile(path):
            print("Loading config from: {}".format(path))
            file_found = 1
            break

    if file_found:
        with open(path) as f:
            my_config = safe_load(f)


print(TAConfig.__dict__)
