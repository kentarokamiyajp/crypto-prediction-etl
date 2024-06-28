import sys
import copy

sys.path.append("/opt/airflow")
from dwh_modules.trino_operations import trino_operator


def run_query(query_file: str):
    """Run from query file

    Args:
        query_file (str): query file path
    """
    with open(query_file, "r") as f:
        trino_ope = trino_operator.TrinoOperator()
        trino_ope.run_query(f.read())


def run_query_by_replacing_params(query_file: str, replacement_params: list):
    """Run from query file with parameter replacement

    Args:
        query_file (str):  query file with params
        replacement_params (list): list of 'param' and 'value' pairs. [{<param1>:<value1>, <param2>:<value2>, ...}, ...]
    """
    with open(query_file, "r") as f:
        trino_ope = trino_operator.TrinoOperator()
        raw_query = f.read()

        # Replace params
        for params in replacement_params:
            query = copy.deepcopy(raw_query)
            for param, value in params.items():
                # replace from <param> to <value>
                query = query.replace(param, value)
            trino_ope.run_query(query)


# def run_from_curr_day_to_N_days_ago(query_file: str, N_days: int):
#     """Run a query for each from current day to N days ago
#     e.g.,
#         today='2024-06-30', N_days = 3
#         Run the query by replacing "${N}" in the query file
#         using each '0'(today), '1'(1 day ago), '2'(2 days ago), '3'(3 days ago) params.

#     Args:
#         query_file (str): Query script path
#         N_days (int): this is the number of how many times to run the query with the N
#     """
#     with open(query_file, "r") as f:
#         trino_ope = trino_operator.TrinoOperator()
#         raw_query = f.read()

#         # Replace the variable "${N}" with an actual value from 0 to N
#         for N in range(0, N_days + 1):
#             query = raw_query.replace("${N}", str(-N))
#             trino_ope.run_query(query)


# def run_from_curr_month_to_N_months_ago(query_file: str, update_N_months_from: int):
#     """Run a query for each month based on ${update_N_months_from}
#     e.g.,
#         today='2024-06-30', update_N_months_from = 3
#         Run the query by replacing "${target_date}" in the query file
#         using each '2024-06', '2024-05', '2024-04', '2024-03' params.

#     Args:
#         query_file (str): query file path
#         update_N_months_from (int): From current month to N months before
#     """
#     with open(query_file, "r") as f:
#         trino_ope = trino_operator.TrinoOperator()
#         query_files = f.read()

#         # Delete data from <update_N_months_from> month ago to the current month
#         today = date.today()
#         first_day_of_curr_month = today.replace(day=1)
#         for i in range(update_N_months_from + 1):
#             # Delete one-month data
#             query = query_files.replace("${target_date}", str(first_day_of_curr_month))
#             trino_ope.run_query(query)

#             # Get the day when one day before <first_day_of_curr_month>
#             last_day_of_prev_month = first_day_of_curr_month - timedelta(days=1)
#             # Get the 1st day of the previous month.
#             first_day_of_curr_month = last_day_of_prev_month.replace(day=1)


# def check_hive_data_deletion(query_file: str, days_delete_from: int):
#     """Check if the target data is deleted properly

#     Args:
#         query_file (str): Query script path
#         days_delete_from (int): How many days before are in the checking scope

#     Raises:
#         AirflowFailException: If the target data is not deleted, raise an exception
#     """
#     with open(query_file, "r") as f:
#         trino_ope = trino_operator.TrinoOperator()
#         query = f.read().replace("${N}", str(-days_delete_from))
#         res = trino_ope.run_query(query)

#         select_count = res[0][0]
#         if select_count != 0:
#             error_msg = "Past data is not deleted !!!\nselect_count:{}".format(select_count)
#             raise AirflowFailException(error_msg)


# def load_from_cassandra_to_hive(query_file: str, days_delete_from: int):
#     """Load data from Cassandra to Hive

#     Args:
#         query_file (str): Query script path
#         days_delete_from (int): How many days before are in the ingesting scope
#     """
#     with open(query_file, "r") as f:
#         trino_ope = trino_operator.TrinoOperator()
#         query = f.read().replace("${N}", str(-days_delete_from))
#         trino_ope.run_query(query)
