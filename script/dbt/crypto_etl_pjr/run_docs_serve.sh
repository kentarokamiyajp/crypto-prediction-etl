#!/bin/bash

profile_dir=/home/git/crypto_prediction_dwh/script/dbt/crypto_etl_pjr
target="dev"
profile="crypto_mart"
log_file="target/dbt_serve.out"

nohup dbt docs serve --profiles-dir ${profile_dir} --target ${target} --profile ${profile} > ${log_file} 2>&1 &

# Access to localhost:8989 (port is fowarded from 8080 to 8989)
