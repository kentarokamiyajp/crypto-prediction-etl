#!/bin/bash

##############################
# Set Config
##############################
ENV_CONF="./env_conf.sh"
if [ ! -e "${ENV_CONF}" ]; then
    echo "Failed to load ${ENV_CONF} !!!"
    exit 1
fi

. "${ENV_CONF}"

DEFAULT_CONF="${COMMON_SCRIPT_HOME}/default_conf.sh"
if [ ! -e "${DEFAULT_CONF}" ]; then
    echo "Failed to load ${DEFAULT_CONF} !!!"
    exit 1
fi

. "${DEFAULT_CONF}"


##############################
# Activate python venv
##############################
if [ ! -d "${PYTHON_ENV_HOME}" ]; then
    echo "Failed, virtualenv doesn't exist (${PYTHON_ENV_HOME}) !!!"
    exit 1
fi

. "${PYTHON_ENV_HOME}/activate"
exec_status=$?
if [ ${exec_status} != 0 ]; then
    echo "Failed to activate python env"
    exit 1
fi

# Set variables
profile_dir=${DBT_PROJECT_HOME}
target="dev"


##############################
# DBT DEBUG
##############################
echo "Running dbt debug ..."
dbt debug --profiles-dir ${profile_dir}
exec_status=$?
if [ ${exec_status} != 0 ]; then
    echo "Failed to run dbt debug !!!"
    exit 1
fi
echo "Finished dbt debug !!!"


##############################
# DBT RUN
##############################
echo "Running dbt run ..."

# create source views
profile="crypto_mart"
model="crypto"
dbt run --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}
exec_status=$?
if [ ${exec_status} != 0 ]; then
    echo "DBT RUN Failed !!!"
    echo "Failed command: 'dbt run --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}'"
    exit 1
fi

echo "Finishied dbt run !!!"

##############################
# DBT TEST
##############################
echo "Running dbt test ..."

# create source views
profile="crypto_mart"
model="crypto"
dbt test --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}
if [ ${exec_status} != 0 ]; then
    echo "DBT TEST Failed !!!"
    echo "Failed command: 'dbt test --profiles-dir ${profile_dir} --target ${target} --profile ${profile} --select ${model}'"
    exit 1
fi

echo "Finishied dbt test !!!"


###############
# Generate Docs
###############
echo "Generating docs ..."

dbt docs generate --profiles-dir ${profile_dir} --target ${target}
if [ ${exec_status} != 0 ]; then
    echo "DBT DOCS Failed !!!"
    echo "Failed command: 'dbt docs generate --profiles-dir ${profile_dir} --target ${target} --profile ${profile}"
    exit 1
fi

echo "Finishied generating docs !!!"



##############################
# Deactivate python venv
##############################
deactivate

exit 0
