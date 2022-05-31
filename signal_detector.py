import time
import pandas as pd
from datetime import timedelta
from logger import log


class signal_detector:

    def __init__(self, dump, symbol,ctx,acct):
        self.dump = dump
        self.symbol = symbol
        self.candle_interval = timedelta(days = 0,seconds = 15*60)
        self.breakout_high = None
        self.breakout_low = None
        self.target = None
        self.stop_loss = None
        self.ctx = ctx
        self.account_id = acct
        # self.order_id = None
        self.pos_units = 10
        self.position = None
        # self.break_out_period
        self.candles = {
            'time_start': [],
            'time_end': [],
            'open': [],
            "high": [],
            "low": [],
            "close": []
        }
    def place_order(self,side,price):
        self.position = side
        if side=="LONG":
            res = ctx.order.market(self.account_id,instrument=self.symbol,units =self.pos_units)
            
        elif side == "SHORT":
            res = ctx.order.market(self.account_id,instrument= self.symbol,units = -self.pos_units)
        
        if res.status == 201:
            log(f"{side} {self.pos_units} {self.symbol} @ {price} ")
        return
    
    def exit_position(self,price):
        if self.position == "LONG":
            res = self.ctx.position.close(self.account_id, instrument=self.symbol,longUnits="ALL")
        elif self.position=="SHORT":
            res = self.ctx.position.close(self.account_id, instrument=self.symbol,shortUnits="ALL")
        if res.status == 200:
            self.position=None
            log(f"EXITING : {self.position} {self.pos_units} {self.symbol} @ {price} ")
        return
    
    def process_candles(self,evt):
        
        while True:
            if evt.is_set():
                df = pd.DataFrame(self.candles)
                df.to_csv(f"candles/{self.symbol}.csv")
                if self.position:
                    self.exit_position(self.dump.prices[self.symbol]['time'][-1])
                return
            if len(self.dump.prices[self.symbol]['time']) == 0:
                continue
            if len(self.candles['time_start']) == 0:
                latest_timestamp = self.dump.prices[self.symbol]['time'][-1]
                next_timestamp = latest_timestamp + self.candle_interval
                open_price = self.dump.prices[self.symbol]['bid'][-1]
                high_price = self.dump.prices[self.symbol]['bid'][-1]
                low_price = self.dump.prices[self.symbol]['bid'][-1]
                close_price = self.dump.prices[self.symbol]['bid'][-1]
                self.candles['time_start'].append(latest_timestamp)
                self.candles['time_end'].append(next_timestamp)
                self.candles['open'].append(open_price)
                self.candles['high'].append(high_price)
                self.candles['low'].append(low_price)
                self.candles['close'].append(close_price)
                

            elif self.dump.prices[self.symbol]['time'][-1] <= self.candles[
                    'time_end'][-1]:
                if self.dump.prices[self.symbol]['bid'][-1] < self.candles['low'][-1]:
                    self.candles['low'][-1] = self.dump.prices[self.symbol]['bid'][
                        -1]
                elif self.dump.prices[self.symbol]['bid'][-1] > self.candles['high'][
                        -1]:
                    self.candles['high'][-1] = self.dump.prices[self.symbol]['bid'][
                        -1]
                self.candles['close'][-1] = self.dump.prices[self.symbol]['bid'][-1]
            elif self.dump.prices[self.symbol]['time'][-1] > self.candles[
                    'end_interval'][-1]:
                latest_timestamp = self.dump.prices[self.symbol]['time'][-1]
                next_timestamp = latest_timestamp + self.candle_interval
                open_price = self.dump.prices[self.symbol]['bid'][-1]
                high_price = self.dump.prices[self.symbol]['bid'][-1]
                low_price = self.dump.prices[self.symbol]['bid'][-1]
                close_price = self.dump.prices[self.symbol]['bid'][-1]
                self.candles['time_start'].append(latest_timestamp)
                self.candles['time_end'].append(next_timestamp)
                self.candles['open'].append(open_price)
                self.candles['high'].append(high_price)
                self.candles['low'].append(low_price)
                self.candles['close'].append(close_price)
                
            if len(self.candles['time_start']) == 2:
                self.breakout_high = self.candles['high'][0]
                self.breakout_low = self.candles['low'][0]
                self.target = 2*self.breakout_high - self.breakout_low
                self.stop_loss = self.breakout_low
            if self.breakout_high and self.dump.prices[self.symbol][-1] > self.breakout_high:
                self.place_order('LONG',self.dump.prices[self.symbol][-1])
            if self.breakout_low and self.dump.prices[self.symbol][-1] < self.breakout_low:
                self.place_order('SHORT',self.dump.prices[self.symbol][-1])
            if (self.stop_loss and self.position and self.stop_loss > self.dump.prices[self.symbol][-1])or(self.target and self.position and self.target < self.dump.prices[self.symbol][-1]):
                self.exit_position(self.dump.prices[self.symbol][-1])

