import time
import signal as sys_signal
import logging
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

from configure import Config
from DataDump import DataDump
from signal_detector import signal_detector

from exit_control import handl

if __name__ == "__main__":
    config = Config()
    config.load()

    logging.basicConfig(filename=f"logs/{datetime.now()}", level=logging.INFO)
    symbols = "EUR_DKK,XAU_CHF,XPD_USD".split(',')
    s_ctx = config.create_streaming_context()
    ctx = config.create_context()
    accounts = config.accounts
    dump = DataDump(s_ctx, accounts)

    evt = Event()
    detectors = [
        signal_detector(dump, sym, ctx, accounts[0]) for sym in symbols
    ]
    sys_signal.signal(sys_signal.SIGINT, handl)

    with ThreadPoolExecutor() as executor:

        stream_t = executor.submit(dump.start_stream, evt)
        detector_ts = []
        for d in detectors:
            t = executor.submit(d.process_candles, evt)
            detector_ts.append(t)

        sys_signal.pause()
        evt.set()