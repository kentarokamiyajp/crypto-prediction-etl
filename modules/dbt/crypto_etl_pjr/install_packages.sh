#!/bin/bash

. ./env_conf.sh
. "${common_home}/default_conf.sh"

# Activate python venv
. /home/venvs/dbt_env/bin/activate

# Set variables
profile_dir=/home/git/crypto_prediction_dwh/modules/dbt/crypto_etl_pjr

# install
dbt deps --profiles-dir ${profile_dir}
