import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import subprocess

base_dir = '/home/kamiken/hadoop_project/crypto/script/spark_streaming'

curr_date = datetime.date.today().strftime("%Y%m%d")
curr_timestamp = str(datetime.datetime.now()).replace(' ','_')

logdir = f'/home/kamiken/kafka/log/{curr_date}'
isExist = os.path.exists(logdir)
if not isExist:
   os.makedirs(logdir)

subprocess.call(f"""nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
                    {base_dir}/streamer_coins_market.py "{curr_date}" "{curr_timestamp}" > \
                    {logdir}/nohup_out_nohup_out_spark_coins_market_{curr_timestamp}.log \
                    2> {logdir}/nohup_error_nohup_out_spark_coins_market_{curr_timestamp}.log &""", shell=True)
