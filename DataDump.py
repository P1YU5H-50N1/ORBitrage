import v20
import pandas as pd
import numpy as np
import time
from datetime import timedelta
import iso8601
# from Exception import KeyboardInterrupt

def date_obj(date_str):
    return iso8601.parse_date(date_str)

def max_weighted_bid(all_bids,time_str):
    timestamp = date_obj(time_str)
    if all_bids:
        bids = [bid.price for bid in all_bids]
        weighted_bids = [
            bid.liquidity * bid.price for bid in all_bids
        ]
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
            } for sym in self.symbols.split(',')
        }
        self.candle_start = None
        self.candle_end = None
        self.candle_interval = timedelta(15*60)
        # self.latest_time = None
        
    def __str__(self):
        return f'DataDump({self.symbols},{self.accounts},{self.prices})'
    def process_candles(self,symbol):
        if len(self.candles[symbol]['time_start']) == 0:
            latest_timestamp = self.prices[symbol][-1]
            next_timestamp = latest_timestamp + self.candle_interval
            open_price = self.prices[symbol]['bid'][-1]
            high_price = self.prices[symbol]['bid'][-1]
            low_price = self.prices[symbol]['bid'][-1]
            close_price = self.prices[symbol]['bid'][-1]
            self.candles[symbol]['time_start'].append(latest_timestamp)
            self.candles[symbol]['time_end'].append(next_timestamp)
            self.candles[symbol]['open'].append(open_price)
            self.candles[symbol]['high'].append(high_price)
            self.candles[symbol]['low'].append(low_price)
            self.candles[symbol]['close'].append(close_price)
            
        elif self.prices[symbol]['time'][-1] <= self.candles[symbol]['end_interval'][-1]:
            if self.prices[symbol]['bid'][-1] < self.candles[symbol]['low'][-1]:
                self.candles[symbol]['low'][-1] =  self.prices[symbol]['bid'][-1]
            elif self.prices[symbol]['bid'][-1] > self.candles[symbol]['high'][-1]:
                self.candles[symbol]['high'][-1] =  self.prices[symbol]['bid'][-1]
            self.candles[symbol]['close'][-1] = self.prices[symbol]['bid'][-1]
        elif self.prices[symbol]['time'][-1] > self.candles[symbol]['end_interval'][-1]:
            latest_timestamp = self.prices[symbol]['time'][-1]
            next_timestamp = latest_timestamp + self.candle_interval
            open_price = self.prices[symbol]['bid'][-1]
            high_price = self.prices[symbol]['bid'][-1]
            low_price = self.prices[symbol]['bid'][-1]
            close_price = self.prices[symbol]['bid'][-1]
            self.candles[symbol]['time_start'].append(latest_timestamp)
            self.candles[symbol]['time_end'].append(next_timestamp)
            self.candles[symbol]['open'].append(open_price)
            self.candles[symbol]['high'].append(high_price)
            self.candles[symbol]['low'].append(low_price)
            self.candles[symbol]['close'].append(close_price)
            

    def start_stream(self):
        try:
            response = self.ctx.pricing.stream(instruments=self.symbols,
                                               accountID=self.accounts[0])
            print(response)
            for msg_type, msg in response.parts():
                if msg_type == 'pricing.ClientPrice':
                    info = max_weighted_bid(msg.bids,msg.time)
                    print(msg.time, type(msg.time))
                    if info:
                        self.prices[msg.instrument]['time'].append(info[0])
                        self.prices[msg.instrument]['bid'].append(info[1])
                    # print(info,msg.instrument)
        except KeyboardInterrupt:
            for instrument, data in self.prices.items():
                df = pd.DataFrame(data)
                df.to_csv(f"{self.dir}/{instrument}.csv")
