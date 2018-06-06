import yaml
import json
import logging
from pathlib import Path

logger = logging.getLogger('trade_agent.config')


def init_fx_info(fx_file):

    logger.info('Getting forex instrument values')

    try:
        with open(fx_file) as f:
            fx_info = json.load(f)
    except FileNotFoundError:
        logger.error('Forex Instrument Config file %s not found...exiting', fx_file)
        exit(1)

    logger.info('Forex instrument values retrieved')
    return fx_info


class OAConf(object):

    def __init__(self, oa_file):

        self.logger = logging.getLogger('trade_agent.config.OAConf')
        self.logger.info('Reading Oanda config file %s', oa_file)

        try:
            with open(oa_file, 'r') as stream:
                conf_dict = yaml.load(stream)
        except FileNotFoundError:
            self.logger.error('Oanda config file %s not found...exiting', oa_file)
            exit(1)
        except yaml.YAMLError as exc:
            self.logger.error(exc)
            exit(1)

        self.c_list = []

        for key, value in conf_dict.items():
            setattr(self, key, value)
            self.c_list.append(key)

        self.logger.info('Oanda config values retrieved')
