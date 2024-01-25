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
# echo "running dbt debug ..."
# dbt debug --profiles-dir ${profile_dir}
# echo "FInished dbt debug !!!"


############
# RUN
############
# echo "running dbt run ..."

# # create source views
# profile="cross_use"
# model="source_view"
# dbt run --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}

# profile="cross_use"
# model="example"
# dbt run --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}

# echo "Finishied dbt run !!!"

############
# TEST
############
echo "running dbt test ..."

# create source views
profile="cross_use"
model="source_view"
dbt test --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}


echo "Finishied dbt test !!!"


# Deactivate
deactivate