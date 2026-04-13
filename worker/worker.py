import os
import time
from datetime import datetime, timedelta
from raw_layer import RawLayerProcessor
from silver_layer import SilverLayerProcessor
from gold_layer import GoldLayerProcessor
from log_utils import calculate_latency

def main():
    startDateTime = datetime.now()
    print("Worker starting...")
    
    raw_processor = RawLayerProcessor()
    silver_processor = SilverLayerProcessor()
    gold_processor = GoldLayerProcessor()

    raw_processor.run()
    silver_processor.run()
    gold_processor.run()

    endDateTime = datetime.now()
    latency = calculate_latency(startDateTime, endDateTime)
    print(f"Worker finished processing. Worker Latency: {latency}")

if __name__ == "__main__":
    main()
