import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))
import requests
import env_variables


def send_line_message(message):
    print('=========================')
    print(env_variables)
    print('=========================')
    url = env_variables.LINE_NOTIFY_URL
    access_token = env_variables.LINE_ACCESS_TOKEN
    headers = {"Authorization": "Bearer " + access_token}
    payload = {"message": message}
    requests.post(
        url,
        headers=headers,
        params=payload,
    )
