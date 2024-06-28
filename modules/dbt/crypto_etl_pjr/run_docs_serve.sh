#!/bin/bash
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


profile_dir=/home/git/crypto_prediction_dwh/modules/dbt/crypto_etl_pjr
target="dev"
profile="crypto_mart"
log_file="target/dbt_serve.out"

nohup dbt docs serve --profiles-dir ${profile_dir} --target ${target} --profile ${profile} > ${log_file} 2>&1 &

# Access to localhost:8989 (port is fowarded from 8080 to 8989)
