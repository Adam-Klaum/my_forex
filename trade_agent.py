import logging
import logging.handlers
import config


def main():

    # TODO move logging setup to a file

    # Setting up global logger
    logfile = 'trade_agent.log'

    logger = logging.getLogger('trade_agent')
    logger.setLevel(logging.DEBUG)

    file_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=5000000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.info('Log Initialization')

    # Retrieving data for each forex instrument
    fx_info = config.init_fx_info('fx_inst.json')

    print(fx_info['EUR_USD']['pip_mult'])

    oa_api = config.init_oa_api(config.OAConf('/home/aklaum/v20.conf'))


if __name__ == "__main__":
    main()




