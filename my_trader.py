import v20
import pandas as pd

class Oanda:

    def __init__(self):
        self.api = v20.Context(
            'stream-fxpractice.oanda.com',
            '443',
            token='38a9bcc1dc54e3e9cf7b50fb7a79c386-ae7eb6b66ed6c7e47b5cbb757e6bc9bc')


oa = Oanda()

response = oa.api.pricing.stream(
    '101-001-8487089-001',
    snapshot=False,
    instruments='EUR_USD'
)


bid_open = 0
bid_close = 0
ask_open = 0
ask_close = 0
stream_start = False

bid_df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close'])
ask_df = bid_df.copy()
bid_index = ask_index = 0
minutes = 5
minute_count = 0
min_bid_list = []
min_ask_list = []
prev_bid = 0
prev_ask = 0
last_minute = -1

def sub_minute(minute):

    if minute == 0:
        return 59
    else:
        return minute - 1

for msg_type, msg in response.parts():
#    if msg_type == "pricing.Heartbeat":
#        print(msg.time)
    if msg_type == "pricing.Price":

        _, minute, _ = msg.time.split(':')
        minute = int(minute)
        if last_minute == -1:
            last_minute = minute

        bid = msg.bids[0].price
        ask = msg.asks[0].price

        # If the minute rolled over, write a record to the DataFrame
        if minute != last_minute:

            bid_open = min_bid_list[0]
            bid_high = max(min_bid_list)
            bid_low = min(min_bid_list)
            bid_close = min_bid_list[-1]

            ask_open = min_ask_list[0]
            ask_high = max(min_ask_list)
            ask_low = max(min_ask_list)
            ask_close = min_ask_list[-1]

            bid_rec = [sub_minute(minute), bid_open, bid_high, bid_low, bid_close]
            ask_rec = [sub_minute(minute), ask_open, ask_high, ask_low, ask_close]

            bid_df.loc[bid_index] = bid_rec
            ask_df.loc[ask_index] = ask_rec

            bid_index += 1
            ask_index += 1

            print()
            print(bid_rec)

            min_bid_list = [bid_close, float(bid)]
            min_ask_list = [ask_close, float(ask)]

            if minute_count == minutes:
                break
            else:
                minute_count += 1

        else:

            min_bid_list.append(float(bid))
            min_ask_list.append(float(ask))


        last_minute = minute
        prev_bid = bid
        prev_ask = ask
        #print('.', end='', flush=True)
        print(msg.instrument + ' ' + str(minute) + ' ' + str(msg.bids[0].price))

print('BID')
print(bid_df)
print('ASK')
print(ask_df)

# instrument
# time
# bids
# asks
