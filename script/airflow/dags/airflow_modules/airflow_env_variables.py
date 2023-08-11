import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv(verbose=True)

dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)

DWH_SCRIPT = os.environ.get("DWH_SCRIPT")
QUERY_SCRIPT = os.environ.get("QUERY_SCRIPT")
