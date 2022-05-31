import v20
import pandas as pd
import numpy as np
import time
import iso8601
import sys
from datetime import timedelta


def date_obj(date_str):
    return iso8601.parse_date(date_str)


def max_weighted_bid(all_bids, time_str):
    timestamp = date_obj(time_str)
    if all_bids:
        bids = [bid.price for bid in all_bids]
        weighted_bids = [bid.liquidity * bid.price for bid in all_bids]
        max_bid_idx = np.argmax(weighted_bids)
        info = [timestamp, bids[max_bid_idx]]
    else:
        info = None

    return info


class DataDump:

    def __init__(self, ctx, accounts):
        self.symbols = "EUR_DKK,XAU_CHF,XPD_USD"
        self.dir = 'prices'
        self.accounts = accounts
        self.ctx = ctx
        self.prices = {
            sym: {
                'time': [],
                'bid': []
            }
            for sym in self.symbols.split(',')
        }
        self.candles = {
            sym: {
                'time_start': [],
                'time_end': [],
                'open': [],
                'high': [],
                'low': [],
                'close': []
            }
            for sym in self.symbols.split(',')
        }
        self.candle_start = None
        self.candle_end = None
        # self.candle_interval = timedelta(15 * 60)
        self.candle_interval = timedelta(60)
        # self.latest_time = None

    def start_stream(self, evt):
        try:
            response = self.ctx.pricing.stream(instruments=self.symbols,
                                               accountID=self.accounts[0])
            for msg_type, msg in response.parts():
                if evt.is_set():
                    for instrument, data in self.prices.items():
                        df = pd.DataFrame(data)
                        df.to_csv(f"{self.dir}/{instrument}.csv")
                    return
                if msg_type == 'pricing.ClientPrice':
                    info = max_weighted_bid(msg.bids, msg.time)
                    if info:
                        self.prices[msg.instrument]['time'].append(info[0])
                        self.prices[msg.instrument]['bid'].append(info[1])
        except Exception as e:
            print("EXCEPTION", e)
            sys.exit(0)
