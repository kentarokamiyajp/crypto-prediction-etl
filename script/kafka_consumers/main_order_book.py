import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import subprocess
import pytz
from common import env_variables

base_dir = env_variables.KAFKA_CONSUMER_HOME

jst = pytz.timezone("Asia/Tokyo")
ts_now = datetime.datetime.now(jst)
curr_date = ts_now.strftime("%Y%m%d")
curr_timestamp = str(ts_now).replace(" ", "_")

logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
isExist = os.path.exists(logdir)
if not isExist:
    os.makedirs(logdir)

consumer_id = "order_book_consumer"

symbol = "ADA_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)

symbol = "BCH_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "BNB_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "BTC_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "DOGE_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "ETH_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "LTC_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "MKR_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "SHIB_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)


symbol = "TRX_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)

symbol = "XRP_USDT"
subprocess.Popen(
    f"""nohup python {base_dir}/{consumer_id}.py "{curr_date}" "{curr_timestamp}" "{consumer_id}" "{symbol}" > \
                    {logdir}/nohup_out_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{consumer_id}_{symbol}_{curr_timestamp}.log &""",
    shell=True,
)