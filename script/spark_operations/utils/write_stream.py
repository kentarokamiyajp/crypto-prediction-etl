import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables

KAFKA_BOOTSTRAP_SERVERS = env_variables.KAFKA_BOOTSTRAP_SERVERS


def start_cassandra_write_stream(
    spark_streamer, transformed_df, checkpoint_location, cassandra_configs
):
    spark_streamer.write_stream = (
        transformed_df.writeStream.outputMode(cassandra_configs["output_mode"])
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(
            lambda df, epoch_id: df.write.format("org.apache.spark.sql.cassandra")
            .options(
                keyspace=cassandra_configs["dest_keyspace"], table=cassandra_configs["dest_table"]
            )
            .mode(cassandra_configs["output_mode"])
            .save()
        )
        .start()
    )
