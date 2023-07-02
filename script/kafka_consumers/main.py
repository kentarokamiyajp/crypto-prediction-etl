import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import subprocess

base_dir = "/home/kamiken/git/crypto_prediction_dwh/script/kafka_consumers"

curr_date = datetime.date.today().strftime("%Y%m%d")
curr_timestamp = str(datetime.datetime.now()).replace(" ", "_")

logdir = f"/home/kamiken/kafka/log/{curr_date}"
isExist = os.path.exists(logdir)
if not isExist:
    os.makedirs(logdir)

subprocess.call(
    f"""nohup python {base_dir}/candles_minute_consumer.py "{curr_date}" "{curr_timestamp}" > \
                    {logdir}/nohup_out_kafka_candles_minute_consumer_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_candles_minute_consumer_{curr_timestamp}.log &""",
    shell=True,
)
