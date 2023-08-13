import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import subprocess
import pytz
from common import env_variables

base_dir = env_variables.KAFKA_PRODUCER_HOME

jst = pytz.timezone("Asia/Tokyo")
ts_now = datetime.datetime.now(jst)
curr_date = ts_now.strftime("%Y%m%d")
curr_timestamp = str(ts_now).replace(" ", "_")

logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
isExist = os.path.exists(logdir)
if not isExist:
    os.makedirs(logdir)
    
producer_id = "order_book_producer"

subprocess.call(
    f"""nohup python {base_dir}/{producer_id}.py "{curr_date}" "{curr_timestamp}" "{producer_id}"> \
                    {logdir}/nohup_out_kafka_{producer_id}_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_kafka_{producer_id}_{curr_timestamp}.log &""",
    shell=True,
)
