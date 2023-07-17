import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import subprocess
import pytz

base_dir = "/home/kamiken/git/crypto_prediction_dwh/script/kafka_producers"

jst = pytz.timezone("Asia/Tokyo")
ts_now = datetime.datetime.now(jst)
curr_date = ts_now.strftime("%Y%m%d")
curr_timestamp = str(ts_now).replace(" ", "_")

logdir = f"/home/kamiken/kafka/log/{curr_date}"
isExist = os.path.exists(logdir)
if not isExist:
    os.makedirs(logdir)

subprocess.call(
    f"""nohup python {base_dir}/candles_minute_producer.py "{curr_date}" "{curr_timestamp}" > \
                    {logdir}/nohup_out_kafka_candles_minute_producer_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_candles_minute_producer_{curr_timestamp}.log &""",
    shell=True,
)
