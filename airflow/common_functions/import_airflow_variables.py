import sys

sys.path.append("/opt/airflow")
from conf.airflow_variables import vars
from airflow.models import Variable


def main():
    """Import variables as "Airflow Variables" from config file

    config file: /opt/airflow/conf/airflow_variables.py
    """
    for key, val in vars.items():
        Variable.set(key=key, value=val)


if __name__ == "__main__":
    main()
