from configure import Config
from DataDump import DataDump
from signal_detector import signal_detector
from concurrent.futures import ThreadPoolExecutor,as_completed

config = Config()
config.load()

ctx = config.create_streaming_context()
accounts = config.accounts
dump = DataDump(ctx,accounts)
detector = signal_detector(dump)
with ThreadPoolExecutor() as executor:
    stream_t = executor.submit(dump.start_stream)
    detector_t = executor.submit(detector.print_prices)