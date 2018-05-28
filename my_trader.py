import v20
import pandas as pd
import oa_config


def sub_minute(minute):
    if minute == 0:
        return 59
    else:
        return minute - 1


def main():
    oa_cf = oa_config.OAConf()

    oa_api = v20.Context(oa_cf.streaming_hostname,
                         oa_cf.port,
                         token=oa_cf.token)

    response = oa_api.pricing.stream(oa_cf.active_account,
                                     snapshot=False,
                                     instruments='USD_CAD')

    bid_df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close'])
    ask_df = bid_df.copy()
    bid_index = ask_index = 0
    minutes = 5
    minute_count = 0
    min_bid_list = []
    min_ask_list = []
    last_minute = -1

    for msg_type, msg in response.parts():

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
            print(msg.instrument + ' ' + str(minute) + ' ' + str(msg.bids[0].price))

    print('BID')
    print(bid_df)
    print('ASK')
    print(ask_df)


if __name__ == "__main__":
    main()
