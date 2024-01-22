#!/bin/bash

. ./env_conf.sh
. "${common_home}/default_conf.sh"

# Activate python venv
. /home/venvs/dbt_env/bin/activate

# Set variables
profile_dir=/home/git/crypto_prediction_dwh/script/dbt/crypto_etl_pjr
target="dev"


############
# Debug
############
echo "running dbt debug ..."
dbt debug --profiles-dir ${profile_dir}


############
# RUN
############
echo "running dbt run ..."

# create staging views
profile="cross_use"
model="staging"
dbt run --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}

profile="cross_use"
model="example"
dbt run --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}


# Deactivate
deactivate