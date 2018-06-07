import logging

logger = logging.getLogger('trade_agent.queue')


class FIFOQueue(list):

    def __init__(self):

        super().__init__()
        self.logger = logging.getLogger('trade_agent.queue.FIFOQueue')
        self.logger.info('Initializing FIFO Queue')

    def type_count(self):

        self.logger.info('Retrieving FIFO Queue counts')

        count_dict = {
            'candles': 0,
            'fills': 0,
            'orders: 0,'
            'signals': 0
        }

        for item in self:

            if item.type == 'CANDLE':
                count_dict['candles'] += 1

            elif item.type == 'FILL':
                count_dict['fills'] += 1

            elif item.type == 'ORDER':
                count_dict['orders'] += 1

            elif item.type == 'SIGNAL':
                count_dict['signals'] += 1

        return count_dict



